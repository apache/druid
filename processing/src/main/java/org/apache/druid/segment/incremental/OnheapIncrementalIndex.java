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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class OnheapIncrementalIndex extends IncrementalIndex
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  /**
   * overhead per {@link ConcurrentSkipListMap.Node} object in facts table
   */
  static final int ROUGH_OVERHEAD_PER_MAP_ENTRY = Long.BYTES * 5 + Integer.BYTES;

  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final FactsHolder facts;
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  protected final int maxRowCount;
  protected final long maxBytesInMemory;

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

  private final SortedSet<AggregateProjectionMetadata> aggregateProjections;
  private final HashMap<String, OnHeapAggregateProjection> projections;

  OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      int maxRowCount,
      long maxBytesInMemory,
      // preserveExistingMetrics should only be set true for DruidInputSource since that is the only case where we can
      // have existing metrics. This is currently only use by auto compaction and should not be use for anything else.
      boolean preserveExistingMetrics
  )
  {
    super(incrementalIndexSchema, preserveExistingMetrics);
    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory == 0 ? Long.MAX_VALUE : maxBytesInMemory;
    if (incrementalIndexSchema.isRollup()) {
      this.facts = new RollupFactsHolder(dimsComparator(), getDimensions(), timePosition == 0);
    } else if (timePosition == 0) {
      this.facts = new PlainTimeOrderedFactsHolder(dimsComparator());
    } else {
      this.facts = new PlainNonTimeOrderedFactsHolder(dimsComparator());
    }

    this.aggregateProjections = new ObjectAVLTreeSet<>(AggregateProjectionMetadata.COMPARATOR);
    this.projections = new HashMap<>();
    initializeProjections(incrementalIndexSchema);
  }

  private void initializeProjections(IncrementalIndexSchema incrementalIndexSchema)
  {
    for (AggregateProjectionSpec projectionSpec : incrementalIndexSchema.getProjections()) {
      // initialize them all with 0 rows
      AggregateProjectionMetadata.Schema schema = projectionSpec.toMetadataSchema();
      aggregateProjections.add(new AggregateProjectionMetadata(schema, 0));

      final OnHeapAggregateProjection projection = new OnHeapAggregateProjection(
          projectionSpec,
          this::getDimension,
          metric -> {
            MetricDesc desc = getMetric(metric);
            if (desc != null) {
              return getMetricAggs()[desc.getIndex()];
            }
            return null;
          },
          incrementalIndexSchema.getMinTimestamp()
      );
      projections.put(projectionSpec.getName(), projection);
    }
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  public Metadata getMetadata()
  {
    if (aggregateProjections.isEmpty()) {
      return super.getMetadata();
    }
    final List<AggregateProjectionMetadata> projectionMetadata = projections.values()
                                                                            .stream()
                                                                            .map(OnHeapAggregateProjection::toMetadata)
                                                                            .collect(Collectors.toList());
    return super.getMetadata().withProjections(projectionMetadata);
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
      InputRowHolder inputRowHolder
  )
  {
    final List<String> parseExceptionMessages = new ArrayList<>();
    final AtomicLong totalSizeInBytes = getBytesInMemory();

    // add to projections first so if one is chosen by queries the data will always be ahead of the base table since
    // rows are not added atomically to all facts holders at once
    for (OnHeapAggregateProjection projection : projections.values()) {
      projection.addToFacts(key, inputRowHolder.getRow(), parseExceptionMessages, totalSizeInBytes);
    }

    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;
    final AggregatorFactory[] metrics = getMetricAggs();
    final AtomicInteger numEntries = getNumEntries();
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = aggregators.get(priorIndex);
      long aggSizeDelta = doAggregate(metrics, aggs, inputRowHolder, parseExceptionMessages);
      totalSizeInBytes.addAndGet(aggSizeDelta);
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

      final int prev = facts.putIfAbsent(key, rowIndex);
      if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) {
        numEntries.incrementAndGet();
      } else {
        throw DruidException.defensive("Encountered existing fact entry for new key, possible concurrent add?");
      }

      // For a new key, row size = key size + aggregator size + overhead
      final long estimatedSizeOfAggregators = aggSizeForRow;
      final long rowSize = key.estimateBytesInMemory()
                           + estimatedSizeOfAggregators
                           + ROUGH_OVERHEAD_PER_MAP_ENTRY;
      totalSizeInBytes.addAndGet(rowSize);
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
      AggregatorAndSize aggregatorAndSize = agg.factorizeWithSize(selectors.get(agg.getName()));
      aggs[i] = aggregatorAndSize.getAggregator();
      totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
      totalInitialSizeBytes += aggReferenceSize;
      // Creates aggregators to combine already aggregated field
      if (preserveExistingMetrics) {
        AggregatorFactory combiningAgg = agg.getCombiningFactory();
        AggregatorAndSize combiningAggAndSize = combiningAgg.factorizeWithSize(combiningAggSelectors.get(combiningAgg.getName()));
        aggs[i + metrics.length] = combiningAggAndSize.getAggregator();
        totalInitialSizeBytes += combiningAggAndSize.getInitialSizeBytes();
        totalInitialSizeBytes += aggReferenceSize;
      }
    }
    return totalInitialSizeBytes;
  }

  /**
   * Performs aggregation for all of the aggregators.
   *
   * @return Total incremental memory in bytes required by this step of the aggregation.
   */
  private long doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      InputRowHolder inputRowHolder,
      List<String> parseExceptionsHolder
  )
  {
    return doAggregate(metrics, aggs, inputRowHolder, parseExceptionsHolder, preserveExistingMetrics);
  }

  static long doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      InputRowHolder inputRowHolder,
      List<String> parseExceptionsHolder,
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
        totalIncrementalBytes += agg.aggregateWithSize();
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
  public QueryableProjection<IncrementalIndexRowSelector> getProjection(CursorBuildSpec buildSpec)
  {
    return Projections.findMatchingProjection(
        buildSpec,
        aggregateProjections,
        (specName, columnName) -> projections.get(specName).getDimensionsMap().containsKey(columnName)
                                  || getColumnCapabilities(columnName) == null,
        projections::get
    );
  }

  @Override
  public IncrementalIndexRowSelector getProjection(String name)
  {
    return projections.get(name);
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean countCheck = numRows() < maxRowCount;
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
          preserveExistingMetrics
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
}
