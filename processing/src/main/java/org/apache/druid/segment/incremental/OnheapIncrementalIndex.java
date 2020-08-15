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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);
  public static final String ADJUST_BYTES_INMEMORY_FLAG = "adjustBytesInMemoryFlag";
  public static final String ADJUST_BYTES_INMEMORY_PERIOD = "adjustBytesInMemoryPeriod";
  public static final int ADJUST_BYTES_INMEMORY_PERIOD_MIN = 1;
  public static final int ADJUST_BYTES_INMEMORY_PERIOD_MAX = 5000;

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
  protected final boolean adjustBytesInMemoryFlag; // control open adjust
  protected final int adjustBytesInMemoryPeriod; // adjust period millis
  protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  protected final List<Integer> indexReadyAdjustRecorder = Collections.synchronizedList(new ArrayList<>());
  protected final ConcurrentHashMap<Integer, Integer[]> indexAdjustedRecorder = new ConcurrentHashMap<>();

  private final AtomicLong atomicRedundantBytesBefore = new AtomicLong(0);
  private volatile long nextRedundantBytes = 0;
  @Nullable
  private volatile Map<String, ColumnSelectorFactory> selectors;
  @Nullable
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
    this.adjustBytesInMemoryFlag = System.getProperty(ADJUST_BYTES_INMEMORY_FLAG) == null
        || "true".equalsIgnoreCase(System.getProperty(ADJUST_BYTES_INMEMORY_FLAG));
    this.adjustBytesInMemoryPeriod = System.getProperty(ADJUST_BYTES_INMEMORY_PERIOD) == null ? 200
        : Math.min(Math.max(Integer.parseInt(System.getProperty(ADJUST_BYTES_INMEMORY_PERIOD)),
        ADJUST_BYTES_INMEMORY_PERIOD_MIN), ADJUST_BYTES_INMEMORY_PERIOD_MAX);
    maxBytesPerRowForAggregators = getMaxBytesPerRowForAggregators(incrementalIndexSchema, canAdjust());
    if (existsAsyncAdjust()) {
      startAsyncAdjust();
    }
    log.debug("adjustBytesInMemoryFlag:[%s],adjustBytesInMemoryPeriod[%s],rowNeedAsyncAdjustAggIndex:%s,rowNeedSyncAdjustAggIndex:%s" +
            "maxBytesPerRowForAggregators[%s]",
        adjustBytesInMemoryFlag, adjustBytesInMemoryPeriod,
        Arrays.toString(rowNeedAsyncAdjustAggIndex),
        Arrays.toString(rowNeedSyncAdjustAggIndex),
        maxBytesPerRowForAggregators);
  }

  public boolean canAdjust()
  {
    return adjustBytesInMemoryFlag && (rowNeedAsyncAdjustAggIndex.length > 0 || rowNeedSyncAdjustAggIndex.length > 0);
  }

  public boolean existsAsyncAdjust()
  {
    return adjustBytesInMemoryFlag && (rowNeedAsyncAdjustAggIndex.length > 0);
  }

  public boolean existsSyncAdjust()
  {
    return adjustBytesInMemoryFlag && rowNeedSyncAdjustAggIndex.length > 0;
  }

  private void startAsyncAdjust()
  {
    scheduler.scheduleWithFixedDelay(
        new Runnable()
        {
          @Override
          public void run()
          {
            long startT = System.currentTimeMillis();
            try {
              final int indexSize = asyncAdjustBytes();
              log.trace("Adjust period[%s],indexSize[%s],cost:%s",
                  adjustBytesInMemoryPeriod, indexSize, System.currentTimeMillis() - startT);
            }
            catch (RuntimeException e) {
              log.error(e, "Uncaught method[asyncAdjustBytes] exception:");
            }
          }

          private int asyncAdjustBytes()
          {
            if (indexReadyAdjustRecorder == null || rowNeedAsyncAdjustAggIndex == null) {
              return 0;
            }
            final HashSet<Integer> distinctIndex;
            final int indexSize = indexReadyAdjustRecorder.size();
            synchronized (indexReadyAdjustRecorder) {
              distinctIndex = new HashSet(indexReadyAdjustRecorder);
              indexReadyAdjustRecorder.clear();
            }
            if (indexSize == 0) {
              return 0;
            }
            long redundantBytesAfter = 0;
            final Iterator<Integer> iterator = distinctIndex.iterator();
            while (iterator.hasNext()) {
              final Integer index = iterator.next();
              redundantBytesAfter += appendBytesInMemoryByAsyncAdjust(index, rowNeedAsyncAdjustAggIndex);
            }
            final long tempRedundBytes = atomicRedundantBytesBefore.addAndGet(redundantBytesAfter);
            nextRedundantBytes = Math.max(nextRedundantBytes, tempRedundBytes);
            return indexSize;
          }
        }, 1, adjustBytesInMemoryPeriod, TimeUnit.MILLISECONDS);
  }

  private long appendBytesInMemoryByAsyncAdjust(final int index, int[] rowNeedAdjustAggIndex)
  {
    long appendBytesTotal = 0;
    Integer[] rowAdjustedCount = indexAdjustedRecorder.computeIfAbsent(index,
        k -> Arrays.stream(new int[rowNeedAdjustAggIndex.length]).boxed().toArray(Integer[]::new));
    for (int ai = 0; ai < rowNeedAdjustAggIndex.length; ai++) { // current row aggs adjust
      final AggregatorFactory[] metrics = getMetrics();
      final Aggregator[] aggs = concurrentGet(index);

      if (aggs == null) {
        log.debug("Aggregators maybe concurrent changed,index:[%s],indexAdjustRecorder.size[%s],aggregators.size[%s],metrics:%s",
            index, indexReadyAdjustRecorder.size(), aggregators.size(), Arrays.toString(metrics));
        continue;
      }
      final MaxIntermediateSizeAdjustStrategy strategy = metrics[rowNeedAdjustAggIndex[ai]].getMaxIntermediateSizeAdjustStrategy();
      final int[] adjustRollupNums = strategy.adjustWithRollupNum();
      final int[] adjustAppendSizes = strategy.appendBytesOnRollupNum();
      final Aggregator agg = aggs[rowNeedAdjustAggIndex[ai]];
      final int curAggCardinalRows = agg.getCardinalRows(); // maybe main cost method
      for (int i = 0; i < adjustRollupNums.length; i++) {
        if (curAggCardinalRows < adjustRollupNums[i]) { // adjustRollupNums need sort by asc
          break;
        }
        if (curAggCardinalRows == adjustRollupNums[i]) { // need adjust: appending bytes[adjustAppendSizes]
          appendBytesTotal += adjustAppendSizes[i];
          log.debug(
              "Current bytes[%s] need add adjustAppendSizes[%s],because curAggCardinalRows[%s] reached adjustRollupNums[%s]",
              getBytesInMemory().get(), adjustAppendSizes[i], curAggCardinalRows, adjustRollupNums[i]);
          rowAdjustedCount[ai]++;
          break;
        }

        // when curAggCardinalRows great then adjustRollupNums handle
        if (curAggCardinalRows > adjustRollupNums[i] && rowAdjustedCount[ai] < i + 1) {
          log.debug(
              "Current bytes[%s] need add adjustAppendSizes[%s],because index[%s] curAggCardinalRows[%s] exceed adjustRollupNums[%s] but rowAdjustedCount[%s]:%s < needAdjust[%s]",
              getBytesInMemory().get(), adjustAppendSizes[i], index, curAggCardinalRows, adjustRollupNums[i], ai, rowAdjustedCount[ai], i + 1);
          appendBytesTotal += adjustAppendSizes[i];
          rowAdjustedCount[ai]++;
        }
      }
    } // end for
    return appendBytesTotal;
  }

  private void appendBytesInMemoryBySyncAdjust(final Aggregator agg, final AggregatorFactory metric)
  {
    if (agg.getCardinalRows() == 0) {
      return;
    }
    final MaxIntermediateSizeAdjustStrategy strategy = metric.getMaxIntermediateSizeAdjustStrategy();
    final int[] adjustRollupNums = strategy.adjustWithRollupNum();
    final int[] adjustAppendSizes = strategy.appendBytesOnRollupNum();
    final int curAggCardinalRows = agg.getCardinalRows(); // maybe main cost method
    for (int i = 0; i < adjustRollupNums.length; i++) {
      if (curAggCardinalRows < adjustRollupNums[i]) { // adjustRollupNums need sort by asc
        break;
      }
      if (curAggCardinalRows == adjustRollupNums[i]) { // need adjust: appending bytes[adjustAppendSizes]
        log.debug(
            "Current bytes[%s] need add adjustAppendSizes[%s],because curAggCardinalRows[%s] reached adjustRollupNums[%s]",
            getBytesInMemory().get(), adjustAppendSizes[i], curAggCardinalRows, adjustRollupNums[i]);
        getBytesInMemory().addAndGet(adjustAppendSizes[i]);
        break;
      }
    }
  }

  public void recordAdjustIndex(final int index)
  {
    if (!canAdjust()) {
      return;
    }
    if (existsAsyncAdjust()) {
      // index record
      indexReadyAdjustRecorder.add(index);
    }
  }

  @VisibleForTesting
  public long getAdjustBytesInMemoryPeriod()
  {
    return adjustBytesInMemoryPeriod;
  }

  @Override
  public void stopAdjust()
  {
    if (!canAdjust()) {
      return;
    }
    scheduler.shutdown();
    indexReadyAdjustRecorder.clear();
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
   * @param canAdjust whether can adjust bytes
   * @return long max aggregator size in bytes
   */
  private static long getMaxBytesPerRowForAggregators(IncrementalIndexSchema incrementalIndexSchema, boolean canAdjust)
  {
    long maxAggregatorIntermediateSize = ((long) Integer.BYTES) * incrementalIndexSchema.getMetrics().length;
    maxAggregatorIntermediateSize += Arrays.stream(incrementalIndexSchema.getMetrics())
                                           .mapToLong(aggregator -> {
                                             if (aggregator.getMaxIntermediateSizeAdjustStrategy() != null && canAdjust) {
                                               final MaxIntermediateSizeAdjustStrategy maxIntermediateSizeAdjustStrategy = aggregator.getMaxIntermediateSizeAdjustStrategy();
                                               return aggregator.getMaxIntermediateSizeWithNulls() + maxIntermediateSizeAdjustStrategy.initAppendBytes()
                                                   + Long.BYTES * 2;
                                             }
                                             return aggregator.getMaxIntermediateSizeWithNulls()
                                                 + Long.BYTES * 2;
                                           })
                                           .sum();
    return maxAggregatorIntermediateSize;
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  protected Aggregator[] initAggs(
      final AggregatorFactory[] metrics,
      final Supplier<InputRow> rowSupplier,
      final boolean deserializeComplexMetrics,
      final boolean concurrentEventAdd
  )
  {
    selectors = new HashMap<>();
    for (AggregatorFactory agg : metrics) {
      selectors.put(
          agg.getName(),
          new CachingColumnSelectorFactory(
              makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics),
              concurrentEventAdd
          )
      );
    }

    return new Aggregator[metrics.length];
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

    Aggregator[] aggs;
    final AggregatorFactory[] metrics = getMetrics();
    final AtomicInteger numEntries = getNumEntries();
    final AtomicLong sizeInBytes = getBytesInMemory();
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = concurrentGet(priorIndex);
      parseExceptionMessages = doAggregate(metrics, aggs, rowContainer, row);
      recordAdjustIndex(priorIndex);
    } else {
      aggs = new Aggregator[metrics.length];
      factorizeAggs(metrics, aggs, rowContainer, row);
      parseExceptionMessages = doAggregate(metrics, aggs, rowContainer, row);

      final int rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, aggs);

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
        aggs = concurrentGet(prev);
        parseExceptionMessages = doAggregate(metrics, aggs, rowContainer, row);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    if (canAdjust()) {
      sizeInBytes.addAndGet(atomicRedundantBytesBefore.getAndSet(0));
      return new AddToFactsResult(numEntries.get(), sizeInBytes.get(), parseExceptionMessages, nextRedundantBytes);
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

  private void factorizeAggs(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row
  )
  {
    rowContainer.set(row);
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggs[i] = agg.factorize(selectors.get(agg.getName()));
    }
    rowContainer.set(null);
  }

  private List<String> doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row
  )
  {
    List<String> parseExceptionMessages = new ArrayList<>();
    rowContainer.set(row);

    for (int i = 0; i < aggs.length; i++) {
      final Aggregator agg = aggs[i];
      synchronized (agg) {
        try {
          agg.aggregate();
          // sync and/or async adjust
          if (existsSyncAdjust()) {
            appendBytesInMemoryBySyncAdjust(agg, metrics[i]);
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

  @Override
  protected Aggregator[] getAggsForRow(int rowOffset)
  {
    return concurrentGet(rowOffset);
  }

  @Override
  protected Object getAggVal(Aggregator agg, int rowOffset, int aggPosition)
  {
    return agg.get();
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getFloat();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].get();
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getDouble();
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].isNull();
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
    scheduler.shutdown();
    indexReadyAdjustRecorder.clear();
    indexAdjustedRecorder.clear();
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

}
