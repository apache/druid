/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.Projections;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 *
 */
public class OnheapIncrementalIndex extends IncrementalIndex
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  /**
   * Constant factor provided to {@link AggregatorFactory#guessAggregatorHeapFootprint(long)} for footprint estimates.
   * This figure is large enough to catch most common rollup ratios, but not so large that it will cause persists to
   * happen too often. If an actual workload involves a much higher rollup ratio, then this may lead to excessive
   * heap usage. Users would have to work around that by lowering maxRowsInMemory or maxBytesInMemory.
   */
  private static final long ROLLUP_RATIO_FOR_AGGREGATOR_FOOTPRINT_ESTIMATION = 100;

  /**
   * overhead per {@link ConcurrentSkipListMap.Node} object in facts table
   */
  private static final int ROUGH_OVERHEAD_PER_MAP_ENTRY = Long.BYTES * 5 + Integer.BYTES;
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final FactsHolder facts;
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  private final long maxBytesPerRowForAggregators;
  protected final int maxRowCount;
  protected final long maxBytesInMemory;

  /**
   * Flag denoting if max possible values should be used to estimate on-heap mem
   * usage.
   * <p>
   * There is one instance of Aggregator per metric per row key.
   * <p>
   * <b>Old Method:</b> {@code useMaxMemoryEstimates = true} (default)
   * <ul>
   *   <li>Aggregator: For a given metric, compute the max memory an aggregator
   *   can use and multiply that by number of aggregators (same as number of
   *   aggregated rows or number of unique row keys)</li>
   *   <li>DimensionIndexer: For each row, encode dimension values and estimate
   *   size of original dimension values</li>
   * </ul>
   *
   * <b>New Method:</b> {@code useMaxMemoryEstimates = false}
   * <ul>
   *   <li>Aggregator: Get the initialize of an Aggregator instance, and add the
   *   incremental size required in each aggregation step.</li>
   *   <li>DimensionIndexer: For each row, encode dimension values and estimate
   *   size of dimension values only if they are newly added to the dictionary</li>
   * </ul>
   * <p>
   * Thus the new method eliminates over-estimations.
   */
  private final boolean useMaxMemoryEstimates;

  /**
   * Aggregator name -> column selector factory for that aggregator.
   */
  @Nullable
  private Map<String, ColumnSelectorFactory> selectors;
  /**
   * Aggregator name -> column selector factory for the combining version of that aggregator. Only set when
   * {@link #preserveExistingMetrics} is true.
   */
  @Nullable
  private Map<String, ColumnSelectorFactory> combiningAggSelectors;
  @Nullable
  private String outOfRowsReason = null;

  private final SortedSet<OnHeapAggregateProjection> aggregateProjections;
  private final HashMap<String, IncrementalIndexRowSelector> projections;

  OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      int maxRowCount,
      long maxBytesInMemory,
      // preserveExistingMetrics should only be set true for DruidInputSource since that is the only case where we can
      // have existing metrics. This is currently only use by auto compaction and should not be use for anything else.
      boolean preserveExistingMetrics,
      boolean useMaxMemoryEstimates
  )
  {
    super(incrementalIndexSchema, preserveExistingMetrics, useMaxMemoryEstimates);
    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory == 0 ? Long.MAX_VALUE : maxBytesInMemory;
    if (incrementalIndexSchema.isRollup()) {
      this.facts = new RollupFactsHolder(dimsComparator(), getDimensions(), timePosition == 0);
    } else if (timePosition == 0) {
      this.facts = new PlainTimeOrderedFactsHolder(dimsComparator());
    } else {
      this.facts = new PlainNonTimeOrderedFactsHolder(dimsComparator());
    }
    this.maxBytesPerRowForAggregators =
        useMaxMemoryEstimates ? getMaxBytesPerRowForAggregators(incrementalIndexSchema) : 0;
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;

    this.aggregateProjections = new ObjectAVLTreeSet<>(OnHeapAggregateProjection.COMPARATOR);
    this.projections = new HashMap<>();
    initializeProjections(incrementalIndexSchema, useMaxMemoryEstimates);
  }

  private void initializeProjections(IncrementalIndexSchema incrementalIndexSchema, boolean useMaxMemoryEstimates)
  {
    for (AggregateProjectionSpec projectionSpec : incrementalIndexSchema.getProjections()) {
      AggregateProjectionSpec schema = projectionSpec;
      final List<DimensionDesc> descs = new ArrayList<>();
      // mapping of position in descs on the projection to position in the parent incremental index. Like the parent
      // incremental index, the time (or time-like) column does not have a dimension descriptor and is specially
      // handled as the timestamp of the row. Unlike the parent incremental index, an aggregating projection will
      // always have its time-like column in the grouping columns list, so its position in this array specifies -1
      final int[] parentDimIndex = new int[schema.getGroupingColumns().size()];
      Arrays.fill(parentDimIndex, -1);
      int i = 0;
      final Map<String, DimensionDesc> dimensionsMap = new HashMap<>();
      for (DimensionSchema dimension : schema.getGroupingColumns()) {
        if (dimension.getName().equals(schema.getTimeColumnName())) {
          continue;
        }
        final DimensionDesc parent = getDimension(dimension.getName());
        if (parent == null) {
          // this dimension only exists in the child, it needs its own handler
          final DimensionDesc childOnly = new DimensionDesc(
              i++,
              dimension.getName(),
              dimension.getDimensionHandler(),
              useMaxMemoryEstimates
          );
          descs.add(childOnly);
          dimensionsMap.put(dimension.getName(), childOnly);
        } else {
          if (!dimension.getColumnType().equals(parent.getCapabilities().toColumnType())) {
            throw InvalidInput.exception(
                "projection[%s] contains dimension[%s] with different type[%s] than type[%s] in base table",
                schema.getName(),
                dimension.getName(),
                dimension.getColumnType(),
                parent.getCapabilities().toColumnType()
            );
          }
          // make a new DimensionDesc from the child, containing all of the parents stuff but with the childs position
          final DimensionDesc child = new DimensionDesc(
              i++,
              parent.getName(),
              parent.getHandler(),
              parent.getIndexer()
          );
          descs.add(child);
          dimensionsMap.put(dimension.getName(), child);
          parentDimIndex[child.getIndex()] = parent.getIndex();
        }
      }
      final OnHeapAggregateProjection projection = new OnHeapAggregateProjection(
          schema,
          descs,
          dimensionsMap,
          parentDimIndex,
          this,
          incrementalIndexSchema.getMinTimestamp(),
          this.useMaxMemoryEstimates,
          this.maxBytesPerRowForAggregators
      );
      aggregateProjections.add(projection);
      projections.put(schema.getName(), projection);
    }
  }

  /**
   * Old method of memory estimation. Used only when {@link #useMaxMemoryEstimates} is true.
   *
   * Gives estimated max size per aggregator. It is assumed that every aggregator will have enough overhead for its own
   * object header and for a pointer to a selector. We are adding a overhead-factor for each object as additional 16
   * bytes.
   * These 16 bytes or 128 bits is the object metadata for 64-bit JVM process and consists of:
   * <ul>
   * <li>Class pointer which describes the object type: 64 bits
   * <li>Flags which describe state of the object including hashcode: 64 bits
   * <ul/>
   * total size estimation consists of:
   * <ul>
   * <li> metrics length : Integer.BYTES * len
   * <li> maxAggregatorIntermediateSize : getMaxIntermediateSize per aggregator + overhead-factor(16 bytes)
   * </ul>
   *
   * @param incrementalIndexSchema
   *
   * @return long max aggregator size in bytes
   */
  private static long getMaxBytesPerRowForAggregators(IncrementalIndexSchema incrementalIndexSchema)
  {
    final long rowsPerAggregator =
        incrementalIndexSchema.isRollup() ? ROLLUP_RATIO_FOR_AGGREGATOR_FOOTPRINT_ESTIMATION : 1;

    long maxAggregatorIntermediateSize = ((long) Integer.BYTES) * incrementalIndexSchema.getMetrics().length;

    for (final AggregatorFactory aggregator : incrementalIndexSchema.getMetrics()) {
      maxAggregatorIntermediateSize +=
          (long) aggregator.guessAggregatorHeapFootprint(rowsPerAggregator) + 2L * Long.BYTES;
    }

    return maxAggregatorIntermediateSize;
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  protected void initAggs(
      final AggregatorFactory[] metrics,
      final InputRowHolder inputRowHolder
  )
  {
    // All non-complex aggregators share a column selector factory. Helps with value reuse.
    ColumnSelectorFactory nonComplexColumnSelectorFactory = null;
    selectors = new HashMap<>();
    combiningAggSelectors = new HashMap<>();
    for (AggregatorFactory agg : metrics) {
      final ColumnSelectorFactory factory;
      if (agg.getIntermediateType().is(ValueType.COMPLEX)) {
        factory = new CachingColumnSelectorFactory(makeColumnSelectorFactory(agg, inputRowHolder));
      } else {
        if (nonComplexColumnSelectorFactory == null) {
          nonComplexColumnSelectorFactory =
              new CachingColumnSelectorFactory(makeColumnSelectorFactory(null, inputRowHolder));
        }
        factory = nonComplexColumnSelectorFactory;
      }
      selectors.put(agg.getName(), factory);
    }

    if (preserveExistingMetrics) {
      for (AggregatorFactory agg : metrics) {
        final AggregatorFactory combiningAgg = agg.getCombiningFactory();
        final ColumnSelectorFactory factory;
        if (combiningAgg.getIntermediateType().is(ValueType.COMPLEX)) {
          factory = new CachingColumnSelectorFactory(makeColumnSelectorFactory(combiningAgg, inputRowHolder));
        } else {
          if (nonComplexColumnSelectorFactory == null) {
            nonComplexColumnSelectorFactory =
                new CachingColumnSelectorFactory(makeColumnSelectorFactory(null, inputRowHolder));
          }
          factory = nonComplexColumnSelectorFactory;
        }
        combiningAggSelectors.put(combiningAgg.getName(), factory);
      }
    }
  }

  @Override
  protected AddToFactsResult addToFacts(
      IncrementalIndexRow key,
      InputRowHolder inputRowHolder,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    final List<String> parseExceptionMessages = new ArrayList<>();
    final AtomicLong totalSizeInBytes = getBytesInMemory();

    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;
    final AggregatorFactory[] metrics = getMetrics();
    final AtomicInteger numEntries = getNumEntries();
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = aggregators.get(priorIndex);
      long aggSizeDelta = doAggregate(metrics, aggs, inputRowHolder, parseExceptionMessages);
      totalSizeInBytes.addAndGet(useMaxMemoryEstimates ? 0 : aggSizeDelta);
    } else {
      if (preserveExistingMetrics) {
        aggs = new Aggregator[metrics.length * 2];
      } else {
        aggs = new Aggregator[metrics.length];
      }
      long aggSizeForRow = factorizeAggs(metrics, aggs);
      aggSizeForRow += doAggregate(metrics, aggs, inputRowHolder, parseExceptionMessages);

      final int rowIndex = indexIncrement.getAndIncrement();
      aggregators.put(rowIndex, aggs);

      // Last ditch sanity checks
      if ((numEntries.get() >= maxRowCount || totalSizeInBytes.get() >= maxBytesInMemory)
          && facts.getPriorIndex(key) == IncrementalIndexRow.EMPTY_ROW_INDEX
          && !skipMaxRowsInMemoryCheck) {
        throw new IndexSizeExceededException(
            "Maximum number of rows [%d] or max size in bytes [%d] reached",
            maxRowCount,
            maxBytesInMemory
        );
      }
      final int prev = facts.putIfAbsent(key, rowIndex);
      if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) {
        numEntries.incrementAndGet();
      } else {
        throw DruidException.defensive("Encountered existing fact entry for new key, possible concurrent add?");
      }

      // For a new key, row size = key size + aggregator size + overhead
      final long estimatedSizeOfAggregators =
          useMaxMemoryEstimates ? maxBytesPerRowForAggregators : aggSizeForRow;
      final long rowSize = key.estimateBytesInMemory()
                           + estimatedSizeOfAggregators
                           + ROUGH_OVERHEAD_PER_MAP_ENTRY;
      totalSizeInBytes.addAndGet(rowSize);
    }

    for (OnHeapAggregateProjection projection : aggregateProjections) {
      projection.addToFacts(key, inputRowHolder.getRow(), parseExceptionMessages, totalSizeInBytes);
    }

    return new AddToFactsResult(numEntries.get(), totalSizeInBytes.get(), parseExceptionMessages);
  }

  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  /**
   * Creates aggregators for the given aggregator factories.
   *
   * @return Total initial size in bytes required by all the aggregators.
   * This value is non-zero only when {@link #useMaxMemoryEstimates} is false.
   */
  private long factorizeAggs(
      AggregatorFactory[] metrics,
      Aggregator[] aggs
  )
  {
    long totalInitialSizeBytes = 0L;
    final long aggReferenceSize = Long.BYTES;
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      // Creates aggregators to aggregate from input into output fields
      if (useMaxMemoryEstimates) {
        aggs[i] = agg.factorize(selectors.get(agg.getName()));
      } else {
        AggregatorAndSize aggregatorAndSize = agg.factorizeWithSize(selectors.get(agg.getName()));
        aggs[i] = aggregatorAndSize.getAggregator();
        totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
        totalInitialSizeBytes += aggReferenceSize;
      }
      // Creates aggregators to combine already aggregated field
      if (preserveExistingMetrics) {
        AggregatorFactory combiningAgg = agg.getCombiningFactory();
        if (useMaxMemoryEstimates) {
          aggs[i + metrics.length] = combiningAgg.factorize(combiningAggSelectors.get(combiningAgg.getName()));
        } else {
          AggregatorAndSize aggregatorAndSize = combiningAgg.factorizeWithSize(combiningAggSelectors.get(combiningAgg.getName()));
          aggs[i + metrics.length] = aggregatorAndSize.getAggregator();
          totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
          totalInitialSizeBytes += aggReferenceSize;
        }
      }
    }
    return totalInitialSizeBytes;
  }

  /**
   * Performs aggregation for all of the aggregators.
   *
   * @return Total incremental memory in bytes required by this step of the
   * aggregation. The returned value is non-zero only if
   * {@link #useMaxMemoryEstimates} is false.
   */
  private long doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      InputRowHolder inputRowHolder,
      List<String> parseExceptionsHolder
  )
  {
    return doAggregate(metrics, aggs, inputRowHolder, parseExceptionsHolder, useMaxMemoryEstimates, preserveExistingMetrics);
  }

  private static long doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      InputRowHolder inputRowHolder,
      List<String> parseExceptionsHolder,
      boolean useMaxMemoryEstimates,
      boolean preserveExistingMetrics
  )
  {
    long totalIncrementalBytes = 0L;
    for (int i = 0; i < metrics.length; i++) {
      final Aggregator agg;
      if (preserveExistingMetrics
          && inputRowHolder.getRow() instanceof MapBasedRow
          && ((MapBasedRow) inputRowHolder.getRow()).getEvent().containsKey(metrics[i].getName())) {
        agg = aggs[i + metrics.length];
      } else {
        agg = aggs[i];
      }
      try {
        if (useMaxMemoryEstimates) {
          agg.aggregate();
        } else {
          totalIncrementalBytes += agg.aggregateWithSize();
        }
      }
      catch (ParseException e) {
        // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
        if (preserveExistingMetrics) {
          log.warn(
              e,
              "Failing ingestion as preserveExistingMetrics is enabled but selector of aggregator[%s] received "
              + "incompatible type.",
              metrics[i].getName()
          );
          throw e;
        } else {
          log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          parseExceptionsHolder.add(e.getMessage());
        }
      }
    }

    return totalIncrementalBytes;
  }

  private void closeAggregators()
  {
    Closer closer = Closer.create();
    for (Aggregator[] aggs : aggregators.values()) {
      for (Aggregator agg : aggs) {
        closer.register(agg);
      }
    }

    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public Projection getProjection(CursorBuildSpec buildSpec)
  {
    if (buildSpec.getQueryContext().getBoolean(QueryContexts.CTX_NO_PROJECTION, false)) {
      return null;
    }
    final String name = buildSpec.getQueryContext().getString(QueryContexts.CTX_USE_PROJECTION);

    if (buildSpec.isAggregate()) {
      for (OnHeapAggregateProjection projection : aggregateProjections) {
        if (name != null && !name.equals(projection.name)) {
          continue;
        }
        final Map<String, String> rewriteColumns = new HashMap<>();
        final Set<VirtualColumn> referenced = new HashSet<>();
        final Projections.PhysicalColumnChecker physicalChecker =
            columnName -> projection.dimensionsMap.containsKey(columnName)
                          || projection.baseTableInspector.getColumnCapabilities(columnName) == null;
        if (projection.matches(buildSpec, referenced, rewriteColumns, physicalChecker)) {
          log.debug("Using projection[%s]", projection.name);
          if (projection.getTimeColumnName() != null) {
            rewriteColumns.put(projection.getTimeColumnName(), ColumnHolder.TIME_COLUMN_NAME);
          }
          // trim virtual columns since some of them may have been remapped to physical projection columns
          final CursorBuildSpec rewrittenBuildSpec =
              CursorBuildSpec.builder(buildSpec)
                             .setVirtualColumns(VirtualColumns.fromIterable(referenced))
                             .build();
          return new Projection(projection, rewrittenBuildSpec, rewriteColumns);
        }
      }
    }
    if (name != null) {
      throw InvalidInput.exception("Projection[%s] specified, but does not satisfy query", name);
    }
    return null;
  }

  @Override
  public IncrementalIndexRowSelector getProjection(String name)
  {
    return projections.get(name);
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean countCheck = size() < maxRowCount;
    // if maxBytesInMemory = -1, then ignore sizeCheck
    final boolean sizeCheck = maxBytesInMemory <= 0 || getBytesInMemory().get() < maxBytesInMemory;
    final boolean canAdd = countCheck && sizeCheck;
    if (!countCheck && !sizeCheck) {
      outOfRowsReason = StringUtils.format(
          "Maximum number of rows [%d] and maximum size in bytes [%d] reached",
          maxRowCount,
          maxBytesInMemory
      );
    } else {
      if (!countCheck) {
        outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
      } else if (!sizeCheck) {
        outOfRowsReason = StringUtils.format("Maximum size in bytes [%d] reached", maxBytesInMemory);
      }
    }

    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return ((Number) getMetricHelper(
        getMetricAggs(),
        aggregators.get(rowOffset),
        aggOffset,
        Aggregator::getFloat
    )).floatValue();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return ((Number) getMetricHelper(
        getMetricAggs(),
        aggregators.get(rowOffset),
        aggOffset,
        Aggregator::getLong
    )).longValue();
  }

  @Override
  public double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return ((Number) getMetricHelper(
        getMetricAggs(),
        aggregators.get(rowOffset),
        aggOffset,
        Aggregator::getDouble
    )).doubleValue();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return getMetricHelper(getMetricAggs(), aggregators.get(rowOffset), aggOffset, Aggregator::get);
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    final Aggregator[] aggs = aggregators.get(rowOffset);
    if (preserveExistingMetrics) {
      return aggs[aggOffset].isNull() && aggs[aggOffset + getMetricAggs().length].isNull();
    } else {
      return aggs[aggOffset].isNull();
    }
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(
      @Nullable final List<PostAggregator> postAggs,
      final boolean descending
  )
  {
    final AggregatorFactory[] metrics = getMetricAggs();

    {
      return () -> {
        final List<DimensionDesc> dimensions = getDimensions();

        return Iterators.transform(
            getFacts().iterator(descending),
            incrementalIndexRow -> {
              final int rowOffset = incrementalIndexRow.getRowIndex();

              Object[] theDims = incrementalIndexRow.getDims();

              Map<String, Object> theVals = Maps.newLinkedHashMap();
              for (int i = 0; i < theDims.length; ++i) {
                Object dim = theDims[i];
                DimensionDesc dimensionDesc = dimensions.get(i);
                if (dimensionDesc == null) {
                  continue;
                }
                String dimensionName = dimensionDesc.getName();
                DimensionHandler handler = dimensionDesc.getHandler();
                if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
                  theVals.put(dimensionName, null);
                  continue;
                }
                final DimensionIndexer indexer = dimensionDesc.getIndexer();
                Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualList(dim);
                theVals.put(dimensionName, rowVals);
              }

              Aggregator[] aggs = aggregators.get(rowOffset);
              int aggLength = preserveExistingMetrics ? aggs.length / 2 : aggs.length;
              for (int i = 0; i < aggLength; ++i) {
                theVals.put(metrics[i].getName(), getMetricHelper(metrics, aggs, i, Aggregator::get));
              }

              if (postAggs != null) {
                for (PostAggregator postAgg : postAggs) {
                  theVals.put(postAgg.getName(), postAgg.compute(theVals));
                }
              }

              return new MapBasedRow(incrementalIndexRow.getTimestamp(), theVals);
            }
        );
      };
    }
  }

  /**
   * Apply the getMetricTypeFunction function to the retrieve aggregated value given the list of aggregators and offset.
   * If preserveExistingMetrics flag is set, then this method will combine values from two aggregators, the aggregator
   * for aggregating from input into output field and the aggregator for combining already aggregated field, as needed
   */
  @Nullable
  private <T> Object getMetricHelper(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      int aggOffset,
      Function<Aggregator, T> getMetricTypeFunction
  )
  {
    if (preserveExistingMetrics) {
      // Since the preserveExistingMetrics flag is set, we will have to check and possibly retrieve the aggregated
      // values from two aggregators, the aggregator for aggregating from input into output field and the aggregator
      // for combining already aggregated field
      if (aggs[aggOffset].isNull()) {
        // If the aggregator for aggregating from input into output field is null, then we get the value from the
        // aggregator that we use for combining already aggregated field
        return getMetricTypeFunction.apply(aggs[aggOffset + metrics.length]);
      } else if (aggs[aggOffset + metrics.length].isNull()) {
        // If the aggregator for combining already aggregated field is null, then we get the value from the
        // aggregator for aggregating from input into output field
        return getMetricTypeFunction.apply(aggs[aggOffset]);
      } else {
        // Since both aggregators is not null and contain values, we will have to retrieve the values from both
        // aggregators and combine them
        AggregatorFactory aggregatorFactory = metrics[aggOffset];
        T aggregatedFromSource = getMetricTypeFunction.apply(aggs[aggOffset]);
        T aggregatedFromCombined = getMetricTypeFunction.apply(aggs[aggOffset + metrics.length]);
        return aggregatorFactory.combine(aggregatedFromSource, aggregatedFromCombined);
      }
    } else {
      // If preserveExistingMetrics flag is not set then we simply get metrics from the list of Aggregator, aggs,
      // using the given aggOffset
      return getMetricTypeFunction.apply(aggs[aggOffset]);
    }
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    closeAggregators();
    aggregators.clear();
    facts.clear();
    if (selectors != null) {
      selectors.clear();
    }
    if (combiningAggSelectors != null) {
      combiningAggSelectors.clear();
    }
  }

  /**
   * Caches references to selector objects for each column instead of creating a new object each time in order to save
   * heap space. In general the selectorFactory need not to thread-safe. If required, set concurrentEventAdd to true to
   * use concurrent hash map instead of vanilla hash map for thread-safe operations.
   */
  static class CachingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final HashMap<String, ColumnValueSelector<?>> columnSelectorMap;
    private final ColumnSelectorFactory delegate;

    public CachingColumnSelectorFactory(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
      this.columnSelectorMap = new HashMap<>();
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      ColumnValueSelector<?> existing = columnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }

      // We cannot use columnSelectorMap.computeIfAbsent(columnName, delegate::makeColumnValueSelector)
      // here since makeColumnValueSelector may modify the columnSelectorMap itself through
      // virtual column references, triggering a ConcurrentModificationException in JDK 9 and above.
      ColumnValueSelector<?> columnValueSelector = delegate.makeColumnValueSelector(columnName);
      existing = columnSelectorMap.putIfAbsent(columnName, columnValueSelector);
      return existing != null ? existing : columnValueSelector;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }
  }

  public static class Builder extends AppendableIndexBuilder
  {
    @Override
    protected OnheapIncrementalIndex buildInner()
    {
      return new OnheapIncrementalIndex(
          Objects.requireNonNull(incrementalIndexSchema, "incrementIndexSchema is null"),
          maxRowCount,
          maxBytesInMemory,
          preserveExistingMetrics,
          useMaxMemoryEstimates
      );
    }
  }

  public static class Spec implements AppendableIndexSpec
  {
    private static final boolean DEFAULT_PRESERVE_EXISTING_METRICS = false;
    public static final String TYPE = "onheap";

    // When set to true, for any row that already has metric (with the same name defined in metricSpec),
    // the metric aggregator in metricSpec is skipped and the existing metric is unchanged. If the row does not already have
    // the metric, then the metric aggregator is applied on the source column as usual. This should only be set for
    // DruidInputSource since that is the only case where we can have existing metrics.
    // This is currently only use by auto compaction and should not be use for anything else.
    final boolean preserveExistingMetrics;

    public Spec()
    {
      this.preserveExistingMetrics = DEFAULT_PRESERVE_EXISTING_METRICS;
    }

    @JsonCreator
    public Spec(
        final @JsonProperty("preserveExistingMetrics") @Nullable Boolean preserveExistingMetrics
    )
    {
      this.preserveExistingMetrics = preserveExistingMetrics != null
                                     ? preserveExistingMetrics
                                     : DEFAULT_PRESERVE_EXISTING_METRICS;
    }

    @JsonProperty
    public boolean isPreserveExistingMetrics()
    {
      return preserveExistingMetrics;
    }

    @Override
    public AppendableIndexBuilder builder()
    {
      return new Builder().setPreserveExistingMetrics(preserveExistingMetrics);
    }

    @Override
    public long getDefaultMaxBytesInMemory()
    {
      // We initially estimated this to be 1/3(max jvm memory), but bytesCurrentlyInMemory only
      // tracks active index and not the index being flushed to disk, to account for that
      // we halved default to 1/6(max jvm memory)
      return JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 6;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Spec spec = (Spec) o;
      return preserveExistingMetrics == spec.preserveExistingMetrics;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(preserveExistingMetrics);
    }
  }

  static final class RollupFactsHolder implements FactsHolder
  {
    // Can't use Set because we need to be able to get from collection
    private final ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow> facts;
    private final List<DimensionDesc> dimensionDescsList;
    private final boolean timeOrdered;
    private volatile long minTime = DateTimes.MAX.getMillis();
    private volatile long maxTime = DateTimes.MIN.getMillis();

    RollupFactsHolder(
        Comparator<IncrementalIndexRow> incrementalIndexRowComparator,
        List<DimensionDesc> dimensionDescsList,
        boolean timeOrdered
    )
    {
      this.facts = new ConcurrentSkipListMap<>(incrementalIndexRowComparator);
      this.dimensionDescsList = dimensionDescsList;
      this.timeOrdered = timeOrdered;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      IncrementalIndexRow row = facts.get(key);
      return row == null ? IncrementalIndexRow.EMPTY_ROW_INDEX : row.getRowIndex();
    }

    @Override
    public long getMinTimeMillis()
    {
      return minTime;
    }

    @Override
    public long getMaxTimeMillis()
    {
      return maxTime;
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      if (descending) {
        return facts.descendingMap()
                    .keySet()
                    .iterator();
      }
      return keySet().iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      if (timeOrdered) {
        IncrementalIndexRow start = new IncrementalIndexRow(timeStart, new Object[]{}, dimensionDescsList);
        IncrementalIndexRow end = new IncrementalIndexRow(timeEnd, new Object[]{}, dimensionDescsList);
        ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow> subMap = facts.subMap(start, end);
        ConcurrentMap<IncrementalIndexRow, IncrementalIndexRow> rangeMap = descending ? subMap.descendingMap() : subMap;
        return rangeMap.keySet();
      } else {
        return Iterables.filter(
            facts.keySet(),
            row -> row.timestamp >= timeStart && row.timestamp < timeEnd
        );
      }
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return facts.keySet();
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      // with rollup, facts are already pre-sorted so just return keyset
      return keySet();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      // setRowIndex() must be called before facts.putIfAbsent() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      minTime = Math.min(minTime, key.timestamp);
      maxTime = Math.max(maxTime, key.timestamp);
      IncrementalIndexRow prev = facts.putIfAbsent(key, key);
      return prev == null ? IncrementalIndexRow.EMPTY_ROW_INDEX : prev.getRowIndex();
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

  static final class PlainTimeOrderedFactsHolder implements FactsHolder
  {
    private final ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>> facts;

    private final Comparator<IncrementalIndexRow> incrementalIndexRowComparator;

    public PlainTimeOrderedFactsHolder(Comparator<IncrementalIndexRow> incrementalIndexRowComparator)
    {
      this.facts = new ConcurrentSkipListMap<>();
      this.incrementalIndexRowComparator = incrementalIndexRowComparator;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      // always return EMPTY_ROW_INDEX to indicate that no prior key cause we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public long getMinTimeMillis()
    {
      return facts.firstKey();
    }

    @Override
    public long getMaxTimeMillis()
    {
      return facts.lastKey();
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      if (descending) {
        return timeOrderedConcat(facts.descendingMap().values(), true).iterator();
      }
      return timeOrderedConcat(facts.values(), false).iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>> subMap = facts.subMap(timeStart, timeEnd);
      final ConcurrentMap<Long, Deque<IncrementalIndexRow>> rangeMap = descending ? subMap.descendingMap() : subMap;
      return timeOrderedConcat(rangeMap.values(), descending);
    }

    private Iterable<IncrementalIndexRow> timeOrderedConcat(
        final Iterable<Deque<IncrementalIndexRow>> iterable,
        final boolean descending
    )
    {
      return () -> Iterators.concat(
          Iterators.transform(
              iterable.iterator(),
              input -> descending ? input.descendingIterator() : input.iterator()
          )
      );
    }

    private Stream<IncrementalIndexRow> timeAndDimsOrderedConcat(
        final Collection<Deque<IncrementalIndexRow>> rowGroups
    )
    {
      return rowGroups.stream()
                      .flatMap(Collection::stream)
                      .sorted(incrementalIndexRowComparator);
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return timeOrderedConcat(facts.values(), false);
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      return () -> timeAndDimsOrderedConcat(facts.values()).iterator();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      Long time = key.getTimestamp();
      Deque<IncrementalIndexRow> rows = facts.get(time);
      if (rows == null) {
        facts.putIfAbsent(time, new ConcurrentLinkedDeque<>());
        // in race condition, rows may be put by other thread, so always get latest status from facts
        rows = facts.get(time);
      }
      // setRowIndex() must be called before rows.add() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      rows.add(key);
      // always return EMPTY_ROW_INDEX to indicate that we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

  static final class PlainNonTimeOrderedFactsHolder implements FactsHolder
  {
    private final Deque<IncrementalIndexRow> facts;
    private final Comparator<IncrementalIndexRow> incrementalIndexRowComparator;
    private volatile long minTime = DateTimes.MAX.getMillis();
    private volatile long maxTime = DateTimes.MIN.getMillis();

    public PlainNonTimeOrderedFactsHolder(Comparator<IncrementalIndexRow> incrementalIndexRowComparator)
    {
      this.facts = new ArrayDeque<>();
      this.incrementalIndexRowComparator = incrementalIndexRowComparator;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      // always return EMPTY_ROW_INDEX to indicate that no prior key cause we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public long getMinTimeMillis()
    {
      return minTime;
    }

    @Override
    public long getMaxTimeMillis()
    {
      return maxTime;
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      return descending ? facts.descendingIterator() : facts.iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      return Iterables.filter(
          () -> iterator(descending),
          row -> row.timestamp >= timeStart && row.timestamp < timeEnd
      );
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return facts;
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      final List<IncrementalIndexRow> sortedFacts = new ArrayList<>(facts);
      sortedFacts.sort(incrementalIndexRowComparator);
      return sortedFacts;
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      // setRowIndex() must be called before rows.add() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      minTime = Math.min(minTime, key.timestamp);
      maxTime = Math.max(maxTime, key.timestamp);
      facts.add(key);
      // always return EMPTY_ROW_INDEX to indicate that we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

  public static class OnHeapAggregateProjection implements IncrementalIndexRowSelector
  {
    static Comparator<OnHeapAggregateProjection> COMPARATOR = (o1, o2) -> {
      return AggregateProjectionSpec.COMPARATOR.compare(o1.projectionSpec, o2.projectionSpec);
    };

    private final AggregateProjectionSpec projectionSpec;
    private final String name;
    private final Granularity projectionGranularity;
    private final boolean hasTimeColumn;
    @Nullable
    private final String timeColumnName;
    private final long minTimestamp;
    private final List<DimensionDesc> dimensions;
    private final VirtualColumns virtualColumns;
    private final AggregatorFactory[] aggregatorFactories;
    private final int[] parentDimensionIndex;
    private final Map<String, DimensionDesc> dimensionsMap;
    private final Map<String, MetricDesc> aggregatorsMap;
    private final List<OrderBy> ordering;
    private final FactsHolder factsHolder;
    private final ColumnInspector baseTableInspector;
    private final InputRowHolder inputRowHolder = new InputRowHolder();
    private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
    private final ColumnSelectorFactory virtualSelectorFactory;
    private final Map<String, ColumnSelectorFactory> aggSelectors;
    private final AtomicInteger rowCounter = new AtomicInteger(0);
    private final boolean useMaxMemoryEstimates;
    private final long maxBytesPerRowForAggregators;
    private final int timePosition;
    private final AtomicInteger numEntries = new AtomicInteger(0);

    public OnHeapAggregateProjection(
        AggregateProjectionSpec schema,
        List<DimensionDesc> dimensions,
        Map<String, DimensionDesc> dimensionsMap,
        int[] parentDimensionIndex,
        ColumnInspector baseTableInspector,
        long minTimestamp,
        boolean useMaxMemoryEstimates,
        long maxBytesPerRowForAggregators
    )
    {
      this.projectionSpec = schema;
      this.name = schema.getName();
      this.dimensions = dimensions;
      this.virtualColumns = schema.getVirtualColumns();
      this.parentDimensionIndex = parentDimensionIndex;
      this.dimensionsMap = dimensionsMap;
      this.baseTableInspector = baseTableInspector;
      this.timeColumnName = projectionSpec.getTimeColumnName();
      this.projectionGranularity = projectionSpec.getGranularity();
      this.hasTimeColumn = projectionSpec.getTimeColumnPosition() >= 0;
      this.timePosition = projectionSpec.getTimeColumnPosition();
      this.minTimestamp = minTimestamp;
      final IncrementalIndexRowComparator rowComparator = new IncrementalIndexRowComparator(
          projectionSpec.getTimeColumnPosition() < 0 ? dimensions.size() : projectionSpec.getTimeColumnPosition(),
          dimensions
      );
      this.factsHolder = new RollupFactsHolder(rowComparator, dimensions, projectionSpec.getTimeColumnPosition() == 0);
      this.useMaxMemoryEstimates = useMaxMemoryEstimates;
      this.maxBytesPerRowForAggregators = maxBytesPerRowForAggregators;

      this.virtualSelectorFactory = new CachingColumnSelectorFactory(
          IncrementalIndex.makeColumnSelectorFactory(schema.getVirtualColumns(), inputRowHolder, null)
      );
      this.aggSelectors = new LinkedHashMap<>();
      this.aggregatorsMap = new LinkedHashMap<>();
      final AggregatorFactory[] metrics = new AggregatorFactory[schema.getAggregators().length];
      int i = 0;
      // all aggregating projections have a count. its cheap and convenient to be able to correctly satisfy any query
      // with a count(*). it may be explicitly defined, but if not, we create one
      boolean hasCount = false;
      for (AggregatorFactory agg : schema.getAggregators()) {
        if (agg instanceof CountAggregatorFactory) {
          hasCount = true;
        }
        MetricDesc metricDesc = new MetricDesc(aggregatorsMap.size(), agg);
        aggregatorsMap.put(metricDesc.getName(), metricDesc);
        final ColumnSelectorFactory factory;
        if (agg.getIntermediateType().is(ValueType.COMPLEX)) {
          factory = new CachingColumnSelectorFactory(
              IncrementalIndex.makeColumnSelectorFactory(VirtualColumns.EMPTY, inputRowHolder, agg)
          );
        } else {
          factory = virtualSelectorFactory;
        }
        aggSelectors.put(agg.getName(), factory);
        metrics[i++] = agg;
      }
      if (!hasCount) {
        this.aggregatorFactories = new AggregatorFactory[metrics.length + 1];
        System.arraycopy(metrics, 0, this.aggregatorFactories, 0, metrics.length);
        AggregatorFactory count = new CountAggregatorFactory("__count");
        aggregatorFactories[metrics.length] = count;
        MetricDesc metricDesc = new MetricDesc(aggregatorsMap.size(), count);
        aggregatorsMap.put(count.getName(), metricDesc);
      } else {
        this.aggregatorFactories = metrics;
      }
      this.ordering = getOrder(dimensions, projectionSpec.getTimeColumnPosition());
    }

    public boolean matches(
        CursorBuildSpec buildSpec,
        Set<VirtualColumn> referencedVirtualColumns,
        Map<String, String> rewriteColumns,
        Projections.PhysicalColumnChecker physicalColumnChecker
    )
    {
      return projectionSpec.matches(buildSpec, referencedVirtualColumns, rewriteColumns, physicalColumnChecker);
    }

    private List<OrderBy> getOrder(List<DimensionDesc> dimensions, int foundTimePosition)
    {
      final ImmutableList.Builder<OrderBy> listBuilder =
          ImmutableList.builderWithExpectedSize(dimensions.size() + 1);
      int i = 0;
      if (i == foundTimePosition) {
        listBuilder.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
      }
      for (String dimName : dimensionsMap.keySet()) {
        listBuilder.add(OrderBy.ascending(dimName));
        i++;
        if (i == foundTimePosition) {
          listBuilder.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
        }
      }
      if (foundTimePosition < 0) {
        listBuilder.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
      }
      return listBuilder.build();
    }

    public void addToFacts(
        IncrementalIndexRow key,
        InputRow inputRow,
        List<String> parseExceptionMessages,
        AtomicLong totalSizeInBytes
    )
    {
      inputRowHolder.set(inputRow);
      final Object[] projectionDims = new Object[dimensions.size()];
      for (int i = 0; i < projectionDims.length; i++) {
        int parentDimIndex = parentDimensionIndex[i];
        if (parentDimIndex < 0) {
          DimensionDesc desc = dimensions.get(i);
          final ColumnValueSelector<?> virtualSelector = virtualSelectorFactory.makeColumnValueSelector(desc.getName());
          EncodedKeyComponent<?> k = desc.getIndexer().processRowValsToUnsortedEncodedKeyComponent(
              virtualSelector.getObject(),
              false
          );
          projectionDims[i] = k.getComponent();
          totalSizeInBytes.addAndGet(k.getEffectiveSizeBytes());
        } else {
          projectionDims[i] = key.dims[parentDimensionIndex[i]];
        }
      }
      final IncrementalIndexRow subKey = new IncrementalIndexRow(
          hasTimeColumn ? projectionGranularity.bucketStart(DateTimes.utc(key.getTimestamp())).getMillis() : minTimestamp,
          projectionDims,
          dimensions
      );

      final int priorIndex = factsHolder.getPriorIndex(subKey);

      final Aggregator[] aggs;
      if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
        aggs = aggregators.get(priorIndex);
        long aggForProjectionSizeDelta = doAggregate(aggregatorFactories, aggs, inputRowHolder, parseExceptionMessages, useMaxMemoryEstimates, false);
        totalSizeInBytes.addAndGet(useMaxMemoryEstimates ? 0 : aggForProjectionSizeDelta);
      } else {
        aggs = new Aggregator[aggregatorFactories.length];
        long aggSizeForProjectionRow = factorizeAggs(aggregatorFactories, aggs);
        aggSizeForProjectionRow += doAggregate(aggregatorFactories, aggs, inputRowHolder, parseExceptionMessages, useMaxMemoryEstimates, false);
        final long estimatedSizeOfAggregators =
            useMaxMemoryEstimates ? maxBytesPerRowForAggregators : aggSizeForProjectionRow;
        final long projectionRowSize = key.estimateBytesInMemory()
                                       + estimatedSizeOfAggregators
                                       + ROUGH_OVERHEAD_PER_MAP_ENTRY;
        totalSizeInBytes.addAndGet(useMaxMemoryEstimates ? 0 : projectionRowSize);
        numEntries.incrementAndGet();
      }
      final int rowIndex = rowCounter.getAndIncrement();
      aggregators.put(rowIndex, aggs);
      factsHolder.putIfAbsent(subKey, rowIndex);
    }

    @Override
    public FactsHolder getFacts()
    {
      return factsHolder;
    }

    @Override
    public List<DimensionDesc> getDimensions()
    {
      return dimensions;
    }

    @Override
    public List<String> getMetricNames()
    {
      return ImmutableList.copyOf(aggregatorsMap.keySet());
    }

    @Override
    public DimensionDesc getDimension(String columnName)
    {
      return dimensionsMap.get(columnName);
    }

    @Override
    public MetricDesc getMetric(String columnName)
    {
      return aggregatorsMap.get(columnName);
    }

    @Override
    public List<OrderBy> getOrdering()
    {
      return ordering;
    }

    @Override
    public int getTimePosition()
    {
      return timePosition;
    }

    @Override
    public boolean isEmpty()
    {
      return rowCounter.get() == 0;
    }

    @Override
    public int getLastRowIndex()
    {
      return rowCounter.get();
    }

    @Override
    public float getMetricFloatValue(int rowOffset, int aggOffset)
    {
      return aggregators.get(rowOffset)[aggOffset].getFloat();
    }

    @Override
    public long getMetricLongValue(int rowOffset, int aggOffset)
    {
      return aggregators.get(rowOffset)[aggOffset].getLong();
    }

    @Override
    public double getMetricDoubleValue(int rowOffset, int aggOffset)
    {
      return aggregators.get(rowOffset)[aggOffset].getDouble();
    }

    @Nullable
    @Override
    public Object getMetricObjectValue(int rowOffset, int aggOffset)
    {
      return aggregators.get(rowOffset)[aggOffset].get();
    }

    @Override
    public boolean isNull(int rowOffset, int aggOffset)
    {
      return aggregators.get(rowOffset)[aggOffset].isNull();
    }

    @Override
    public ColumnFormat getColumnFormat(String columnName)
    {
      // todo (clint): hella jank
      DimensionDesc dimensionDesc = getDimension(columnName);
      if (dimensionDesc == null) {
        if (columnName.equals(timeColumnName)) {
          return new CapabilitiesBasedFormat(ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG));
        }
        MetricDesc metricDesc = getMetric(columnName);
        if (metricDesc == null) {
          return null;
        }
        return new CapabilitiesBasedFormat(metricDesc.getCapabilities());
      }
      return dimensionDesc.getIndexer().getFormat();
    }

    @Override
    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    @Override
    public int size()
    {
      return numEntries.get();
    }

    @Override
    public List<String> getDimensionNames(boolean includeTime)
    {
      synchronized (dimensionsMap) {
        if (includeTime) {
          final ImmutableList.Builder<String> listBuilder =
              ImmutableList.builderWithExpectedSize(dimensionsMap.size() + 1);
          int i = 0;
          if (i == timePosition) {
            listBuilder.add(timeColumnName);
          }
          for (String dimName : dimensionsMap.keySet()) {
            listBuilder.add(dimName);
            i++;
            if (i == timePosition) {
              listBuilder.add(timeColumnName);
            }
          }
          return listBuilder.build();
        } else {
          return ImmutableList.copyOf(dimensionsMap.keySet());
        }
      }
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(column) || Objects.equals(column, timeColumnName)) {
        return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG).setHasNulls(false);
      }
      if (dimensionsMap.containsKey(column)) {
        return dimensionsMap.get(column).getCapabilities();
      }
      if (aggregatorsMap.containsKey(column)) {
        return aggregatorsMap.get(column).getCapabilities();
      }
      return null;
    }

    private long factorizeAggs(AggregatorFactory[] aggregatorFactories, Aggregator[] aggs)
    {
      long totalInitialSizeBytes = 0L;
      final long aggReferenceSize = Long.BYTES;
      for (int i = 0; i < aggregatorFactories.length; i++) {
        final AggregatorFactory agg = aggregatorFactories[i];
        // Creates aggregators to aggregate from input into output fields
        if (useMaxMemoryEstimates) {
          aggs[i] = agg.factorize(aggSelectors.get(agg.getName()));
        } else {
          AggregatorAndSize aggregatorAndSize = agg.factorizeWithSize(aggSelectors.get(agg.getName()));
          aggs[i] = aggregatorAndSize.getAggregator();
          totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
          totalInitialSizeBytes += aggReferenceSize;
        }
      }
      return totalInitialSizeBytes;
    }
  }
}
