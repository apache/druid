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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class OnheapIncrementalIndex extends ExternalDataIncrementalIndex<Aggregator>
{
  /**
   * overhead per {@link ConcurrentHashMap.Node}  or {@link java.util.concurrent.ConcurrentSkipListMap.Node} object
   */
  private static final int ROUGH_OVERHEAD_PER_MAP_ENTRY = Long.BYTES * 5 + Integer.BYTES;

  protected final AtomicInteger indexIncrement = new AtomicInteger(0);
  private final long maxBytesPerRowForAggregators;
  protected final long maxBytesInMemory;
  protected OnheapAggsManager aggsManager;

  OnheapIncrementalIndex(
          IncrementalIndexSchema incrementalIndexSchema,
          boolean deserializeComplexMetrics,
          boolean reportParseExceptions,
          boolean concurrentEventAdd,
          boolean sortFacts,
          int maxRowCount,
          long maxBytesInMemory
  )
  {
    super(incrementalIndexSchema, reportParseExceptions, sortFacts, maxRowCount);
    this.aggsManager = new OnheapAggsManager(incrementalIndexSchema, deserializeComplexMetrics,
            reportParseExceptions, concurrentEventAdd, rowSupplier, columnCapabilities, this);
    this.maxBytesInMemory = maxBytesInMemory == 0 ? Long.MAX_VALUE : maxBytesInMemory;
    maxBytesPerRowForAggregators = getMaxBytesPerRowForAggregators(incrementalIndexSchema);
  }

  /**
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
    long maxAggregatorIntermediateSize = Integer.BYTES * incrementalIndexSchema.getMetrics().length;
    maxAggregatorIntermediateSize += Arrays.stream(incrementalIndexSchema.getMetrics())
                                           .mapToLong(aggregator -> aggregator.getMaxIntermediateSizeWithNulls() + Long.BYTES * 2)
                                           .sum();
    return maxAggregatorIntermediateSize;
  }

  @Override
  protected FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  public Aggregator[] getAggs()
  {
    return aggsManager.getAggs();
  }

  @Override
  public AggregatorFactory[] getMetricAggs()
  {
    return aggsManager.getMetricAggs();
  }

  @Override
  protected AddToFactsResult addToFacts(
          boolean reportParseExceptions,
          InputRow row,
          AtomicInteger numEntries,
          AtomicLong sizeInBytes,
          IncrementalIndexRow key,
          ThreadLocal<InputRow> rowContainer,
          Supplier<InputRow> rowSupplier,
          boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    List<String> parseExceptionMessages;
    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;

    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = aggsManager.concurrentGet(priorIndex);
      parseExceptionMessages = aggsManager.doAggregate(aggsManager.metrics, aggs, rowContainer, row);
    } else {
      aggs = new Aggregator[aggsManager.metrics.length];
      aggsManager.factorizeAggs(aggsManager.metrics, aggs, rowContainer, row);
      parseExceptionMessages = aggsManager.doAggregate(aggsManager.metrics, aggs, rowContainer, row);

      final int rowIndex = indexIncrement.getAndIncrement();
      aggsManager.concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if ((numEntries.get() >= maxRowCount || sizeInBytes.get() >= maxBytesInMemory)
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
        long estimatedRowSize = estimateRowSizeInBytes(key, maxBytesPerRowForAggregators);
        sizeInBytes.addAndGet(estimatedRowSize);
      } else {
        // We lost a race
        aggs = aggsManager.concurrentGet(prev);
        parseExceptionMessages = aggsManager.doAggregate(aggsManager.metrics, aggs, rowContainer, row);
        // Free up the misfire
        aggsManager.concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    return new AddToFactsResult(numEntries.get(), sizeInBytes.get(), parseExceptionMessages);
  }

  /**
   * Gives an estimated size of row in bytes, it accounts for:
   * <ul>
   * <li> overhead per Map Entry
   * <li> TimeAndDims key size
   * <li> aggregator size
   * </ul>
   *
   * @param key                          TimeAndDims key
   * @param maxBytesPerRowForAggregators max size per aggregator
   *
   * @return estimated size of row
   */
  private long estimateRowSizeInBytes(IncrementalIndexRow key, long maxBytesPerRowForAggregators)
  {
    return ROUGH_OVERHEAD_PER_MAP_ENTRY + key.estimateBytesInMemory() + maxBytesPerRowForAggregators;
  }

  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean countCheck = size() < maxRowCount;
    // if maxBytesInMemory = -1, then ignore sizeCheck
    final boolean sizeCheck = maxBytesInMemory <= 0 || getBytesInMemory() < maxBytesInMemory;
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
  protected Aggregator[] getAggsForRow(IncrementalIndexRow incrementalIndexRow)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex);
  }

  @Override
  protected Object getAggVal(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].get();
  }

  @Override
  public float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].getFloat();
  }

  @Override
  public long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].getLong();
  }

  @Override
  public Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].get();
  }

  @Override
  public double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].getDouble();
  }

  @Override
  public boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int rowIndex = incrementalIndexRow.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].isNull();
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    aggsManager.closeAggregators();
    aggsManager.clearAggregators();
    facts.clear();
    aggsManager.clearSelectors();
  }

  // Caches references to selector objects for each column instead of creating a new object each time in order to save heap space.
  // In general the selectorFactory need not to thread-safe.
  // If required, set concurrentEventAdd to true to use concurrent hash map instead of vanilla hash map for thread-safe
  // operations.
  static class ObjectCachingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final Map<String, ColumnValueSelector<?>> columnSelectorMap;
    private final ColumnSelectorFactory delegate;

    public ObjectCachingColumnSelectorFactory(ColumnSelectorFactory delegate, boolean concurrentEventAdd)
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
      final ColumnValueSelector existing = columnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }
      return columnSelectorMap.computeIfAbsent(columnName, delegate::makeColumnValueSelector);
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }
  }

  @Nullable
  @Override
  public String getMetricType(String metric)
  {
    return aggsManager.getMetricType(metric);
  }

  @Override
  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry)
  {
    return aggsManager.makeMetricColumnValueSelector(metric, currEntry);
  }

  @Override
  public List<String> getMetricNames()
  {
    return aggsManager.getMetricNames();
  }

  @Override
  protected String getMetricName(int metricIndex)
  {
    return aggsManager.metrics[metricIndex].getName();
  }

}
