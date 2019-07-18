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

import com.google.common.base.Supplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.memory.BufferHolder;
import org.apache.druid.memory.MemoryAllocator;
import org.apache.druid.memory.SimpleOnHeapMemoryAllocator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class OnheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);
  /**
   * overhead per {@link ConcurrentHashMap.Node}  or {@link java.util.concurrent.ConcurrentSkipListMap.Node} object
   */
  private static final int ROUGH_OVERHEAD_PER_MAP_ENTRY = Long.BYTES * 5 + Integer.BYTES;
  private final ConcurrentHashMap<Integer, BufferHolder[]> aggregators = new ConcurrentHashMap<>();
  private final FactsHolder facts;
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  private final long initialBytesPerRowForAggregators;
  protected final int maxRowCount;
  protected final long maxBytesInMemory;
  private final MemoryAllocator memoryAllocator = new SimpleOnHeapMemoryAllocator();

  private String outOfRowsReason = null;

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
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory == 0 ? Long.MAX_VALUE : maxBytesInMemory;
    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts, dimsComparator());
    initialBytesPerRowForAggregators = getInitialBytesPerRowForAggregators(incrementalIndexSchema);
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
  private static long getInitialBytesPerRowForAggregators(IncrementalIndexSchema incrementalIndexSchema)
  {
    long initialSize = Integer.BYTES * incrementalIndexSchema.getMetrics().length;
    initialSize += Arrays.stream(incrementalIndexSchema.getMetrics())
                                           .mapToLong(aggregator -> aggregator.getMinIntermediateSize()
                                                                    + Long.BYTES * 2)
                                           .sum();
    return initialSize;
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  protected BufferAggregator[] initAggs(
      final AggregatorFactory[] metrics,
      final Supplier<InputRow> rowSupplier,
      final boolean deserializeComplexMetrics,
      final boolean concurrentEventAdd
  )
  {
    BufferAggregator[] aggs = new BufferAggregator[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      aggs[i] = metrics[i].factorizeBuffered(new CachingColumnSelectorFactory(
          makeColumnSelectorFactory(metrics[i], rowSupplier, deserializeComplexMetrics),
          concurrentEventAdd
      ));
    }

    return aggs;
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
    List<String> parseExceptionMessages;
    final int priorIndex = facts.getPriorIndex(key);

    BufferHolder[] bufferHolders;
    final AggregatorFactory[] metrics = getMetrics();
    final AtomicInteger numEntries = getNumEntries();
    final AtomicLong sizeInBytes = getBytesInMemory();
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      bufferHolders = concurrentGet(priorIndex);
      parseExceptionMessages = doAggregate(metrics, getAggs(), bufferHolders, rowContainer, row);
    } else {
      bufferHolders = factorizeAggs(metrics, getAggs(), rowContainer, row);
      parseExceptionMessages = doAggregate(metrics, getAggs(), bufferHolders, rowContainer, row);

      final int rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, bufferHolders);

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
        long estimatedRowSize = estimateRowSizeInBytes(key, initialBytesPerRowForAggregators);
        sizeInBytes.addAndGet(estimatedRowSize);
      } else {
        // We lost a race
        bufferHolders = concurrentGet(prev);
        parseExceptionMessages = doAggregate(metrics, getAggs(), bufferHolders, rowContainer, row);
        // Free up the misfire
        concurrentRemove(rowIndex);
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
   * @param initialBytesPerRowForAggregators initial size for aggregators per row
   *
   * @return estimated size of row
   */
  private long estimateRowSizeInBytes(IncrementalIndexRow key, long initialBytesPerRowForAggregators)
  {
    return ROUGH_OVERHEAD_PER_MAP_ENTRY + key.estimateBytesInMemory() + initialBytesPerRowForAggregators;
  }

  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  private BufferHolder[] factorizeAggs(
      AggregatorFactory[] metrics,
      BufferAggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row
  )
  {
    rowContainer.set(row);

    BufferHolder[] buffs = new BufferHolder[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      buffs[i] = memoryAllocator.allocate(metrics[i].getMinIntermediateSize());
      aggs[i].init(buffs[i].get(), buffs[i].position(), buffs[i].capacity());
    }
    rowContainer.set(null);
    return buffs;
  }

  private List<String> doAggregate(
      AggregatorFactory[] metrics,
      BufferAggregator[] aggs,
      BufferHolder[] bufferHolders,
      ThreadLocal<InputRow> rowContainer,
      InputRow row
  )
  {
    List<String> parseExceptionMessages = new ArrayList<>();
    rowContainer.set(row);

    for (int i = 0; i < aggs.length; i++) {
      synchronized (aggs[i]) {
        try {
          while (!aggs[i].aggregate(bufferHolders[i].get(), bufferHolders[i].position(), bufferHolders[i].capacity())) {
            BufferHolder old = bufferHolders[i];
            BufferHolder bigger = memoryAllocator.allocate(2*old.capacity());
            aggs[i].relocate(old.get(), old.position(), old.capacity(), bigger.get(), bigger.position(), bigger.capacity());
            getBytesInMemory().addAndGet(bigger.capacity() - old.capacity());
            memoryAllocator.free(old);
          }
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          parseExceptionMessages.add(e.getMessage());
        }
      }
    }

    rowContainer.set(null);
    return parseExceptionMessages;
  }

  private void closeAggregators()
  {
    Closer closer = Closer.create();
    for (BufferHolder[] bufferHolders : aggregators.values()) {
      for (BufferHolder bh : bufferHolders) {
        closer.register(() -> memoryAllocator.free(bh));
      }
    }

    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected BufferHolder[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  protected void concurrentSet(int offset, BufferHolder[] value)
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

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    BufferHolder bh = concurrentGet(rowOffset)[aggOffset];
    return getAggs()[aggOffset].getFloat(bh.get(), bh.position(), bh.capacity());
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    BufferHolder bh = concurrentGet(rowOffset)[aggOffset];
    return getAggs()[aggOffset].getLong(bh.get(), bh.position(), bh.capacity());
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    BufferHolder bh = concurrentGet(rowOffset)[aggOffset];
    return getAggs()[aggOffset].get(bh.get(), bh.position(), bh.capacity());
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    BufferHolder bh = concurrentGet(rowOffset)[aggOffset];
    return getAggs()[aggOffset].getDouble(bh.get(), bh.position(), bh.capacity());
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    BufferHolder bh = concurrentGet(rowOffset)[aggOffset];
    return getAggs()[aggOffset].isNull(bh.get(), bh.position(), bh.capacity());
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

}
