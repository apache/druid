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
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
   * overhead per {@link ConcurrentHashMap.Node}  or {@link java.util.concurrent.ConcurrentSkipListMap.Node} object
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

  @Nullable
  private volatile Map<String, ColumnSelectorFactory> selectors;
  @Nullable
  private volatile Map<String, ColumnSelectorFactory> combiningAggSelectors;
  @Nullable
  private String outOfRowsReason = null;

  OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd,
      boolean sortFacts,
      int maxRowCount,
      long maxBytesInMemory,
      // preserveExistingMetrics should only be set true for DruidInputSource since that is the only case where we can have existing metrics
      // This is currently only use by auto compaction and should not be use for anything else.
      boolean preserveExistingMetrics,
      boolean useMaxMemoryEstimates
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, concurrentEventAdd, preserveExistingMetrics, useMaxMemoryEstimates);
    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory == 0 ? Long.MAX_VALUE : maxBytesInMemory;
    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts, dimsComparator());
    maxBytesPerRowForAggregators =
        useMaxMemoryEstimates ? getMaxBytesPerRowForAggregators(incrementalIndexSchema) : 0;
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
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
      final Supplier<InputRow> rowSupplier,
      final boolean deserializeComplexMetrics,
      final boolean concurrentEventAdd
  )
  {
    selectors = new HashMap<>();
    combiningAggSelectors = new HashMap<>();
    for (AggregatorFactory agg : metrics) {
      selectors.put(
          agg.getName(),
          new CachingColumnSelectorFactory(
              makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics),
              concurrentEventAdd
          )
      );
      if (preserveExistingMetrics) {
        AggregatorFactory combiningAgg = agg.getCombiningFactory();
        combiningAggSelectors.put(
            combiningAgg.getName(),
            new CachingColumnSelectorFactory(
                makeColumnSelectorFactory(combiningAgg, rowSupplier, deserializeComplexMetrics),
                concurrentEventAdd
            )
        );
      }
    }
  }

  @Override
  protected AddToFactsResult addToFacts(
      InputRow row,
      IncrementalIndexRow key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    final List<String> parseExceptionMessages = new ArrayList<>();
    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;
    final AggregatorFactory[] metrics = getMetrics();
    final AtomicInteger numEntries = getNumEntries();
    final AtomicLong totalSizeInBytes = getBytesInMemory();
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = concurrentGet(priorIndex);
      long aggSizeDelta = doAggregate(metrics, aggs, rowContainer, row, parseExceptionMessages);
      totalSizeInBytes.addAndGet(useMaxMemoryEstimates ? 0 : aggSizeDelta);
    } else {
      if (preserveExistingMetrics) {
        aggs = new Aggregator[metrics.length * 2];
      } else {
        aggs = new Aggregator[metrics.length];
      }
      long aggSizeForRow = factorizeAggs(metrics, aggs, rowContainer, row);
      aggSizeForRow += doAggregate(metrics, aggs, rowContainer, row, parseExceptionMessages);

      final int rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, aggs);

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
        // This would happen in a race condition where there are multiple write threads
        // which could be possible in case of GroupBy v1 strategy
        parseExceptionMessages.clear();
        aggs = concurrentGet(prev);
        aggSizeForRow = doAggregate(metrics, aggs, rowContainer, row, parseExceptionMessages);

        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }

      // For a new key, row size = key size + aggregator size + overhead
      final long estimatedSizeOfAggregators =
          useMaxMemoryEstimates ? maxBytesPerRowForAggregators : aggSizeForRow;
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
   * This value is non-zero only when {@link #useMaxMemoryEstimates} is false.
   */
  private long factorizeAggs(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row
  )
  {
    long totalInitialSizeBytes = 0L;
    rowContainer.set(row);
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
        if (useMaxMemoryEstimates) {
          AggregatorFactory combiningAgg = agg.getCombiningFactory();
          aggs[i + metrics.length] = combiningAgg.factorize(combiningAggSelectors.get(combiningAgg.getName()));
        } else {
          AggregatorFactory combiningAgg = agg.getCombiningFactory();
          AggregatorAndSize aggregatorAndSize = combiningAgg.factorizeWithSize(combiningAggSelectors.get(combiningAgg.getName()));
          aggs[i + metrics.length] = aggregatorAndSize.getAggregator();
          totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
          totalInitialSizeBytes += aggReferenceSize;
        }
      }
    }
    rowContainer.set(null);
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
      ThreadLocal<InputRow> rowContainer,
      InputRow row,
      List<String> parseExceptionsHolder
  )
  {
    rowContainer.set(row);
    long totalIncrementalBytes = 0L;
    for (int i = 0; i < metrics.length; i++) {
      final Aggregator agg;
      if (preserveExistingMetrics && row instanceof MapBasedRow && ((MapBasedRow) row).getEvent().containsKey(metrics[i].getName())) {
        agg = aggs[i + metrics.length];
      } else {
        agg = aggs[i];
      }
      synchronized (agg) {
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
            log.warn(e, "Failing ingestion as preserveExistingMetrics is enabled but selector of aggregator[%s] recieved incompatible type.", metrics[i].getName());
            throw e;
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
            parseExceptionsHolder.add(e.getMessage());
          }
        }
      }
    }

    rowContainer.set(null);
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

  protected Aggregator[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  protected void concurrentSet(int offset, Aggregator[] value)
  {
    aggregators.put(offset, value);
  }

  protected void concurrentRemove(int offset)
  {
    aggregators.remove(offset);
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

  protected Aggregator[] getAggsForRow(int rowOffset)
  {
    return concurrentGet(rowOffset);
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return ((Number) getMetricHelper(getMetricAggs(), concurrentGet(rowOffset), aggOffset, Aggregator::getFloat)).floatValue();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return ((Number) getMetricHelper(getMetricAggs(), concurrentGet(rowOffset), aggOffset, Aggregator::getLong)).longValue();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return getMetricHelper(getMetricAggs(), concurrentGet(rowOffset), aggOffset, Aggregator::get);
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return ((Number) getMetricHelper(getMetricAggs(), concurrentGet(rowOffset), aggOffset, Aggregator::getDouble)).doubleValue();
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    if (preserveExistingMetrics) {
      return concurrentGet(rowOffset)[aggOffset].isNull() && concurrentGet(rowOffset)[aggOffset + getMetricAggs().length].isNull();
    } else {
      return concurrentGet(rowOffset)[aggOffset].isNull();
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

              Aggregator[] aggs = getAggsForRow(rowOffset);
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
  private <T> Object getMetricHelper(AggregatorFactory[] metrics, Aggregator[] aggs, int aggOffset, Function<Aggregator, T> getMetricTypeFunction)
  {
    if (preserveExistingMetrics) {
      // Since the preserveExistingMetrics flag is set, we will have to check and possibly retrieve the aggregated values
      // from two aggregators, the aggregator for aggregating from input into output field and the aggregator
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
      // If preserveExistingMetrics flag is not set then we simply get metrics from the list of Aggregator, aggs, using the
      // given aggOffset
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
    private final Map<String, ColumnValueSelector<?>> columnSelectorMap;
    private final ColumnSelectorFactory delegate;

    public CachingColumnSelectorFactory(ColumnSelectorFactory delegate, boolean concurrentEventAdd)
    {
      this.delegate = delegate;

      if (concurrentEventAdd) {
        columnSelectorMap = new ConcurrentHashMap<>();
      } else {
        columnSelectorMap = new HashMap<>();
      }
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      ColumnValueSelector existing = columnSelectorMap.get(columnName);
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
          deserializeComplexMetrics,
          concurrentEventAdd,
          sortFacts,
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
      this.preserveExistingMetrics = preserveExistingMetrics != null ? preserveExistingMetrics : DEFAULT_PRESERVE_EXISTING_METRICS;
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
}
