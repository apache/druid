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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);
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
  protected final boolean adjustmentBytesInMemoryFlag; // control adjustment
  protected final int adjustmentBytesInMemoryMaxRollupRows; // adjust when reach max rollup row
  protected final int adjustmentBytesInMemoryMaxTimeMs; // adjust period millis
  protected final int[] rowNeedAsyncAdjustAggIndex;
  protected final int[] rowNeedSyncAdjustAggIndex;
  protected final ConcurrentHashMap<Integer, Integer[]> indexAdjustedRecorder = new ConcurrentHashMap<>();
  private final BlockingQueue<List<Integer>> indexReadyListQueue = new LinkedBlockingQueue<>(2);
  private final AtomicLong atomicCurrentNeedAppendBytes = new AtomicLong(0);
  protected volatile List<Integer> indexReadyAdjustRecorder = Collections.synchronizedList(new ArrayList<>());
  private volatile ListeningExecutorService adjustExecutor = null;
  private volatile long adjustBeforeTime;

  @Nullable
  private volatile Map<String, ColumnSelectorFactory> selectors;
  @Nullable
  private String outOfRowsReason = null;

  OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd,
      boolean sortFacts,
      int maxRowCount,
      long maxBytesInMemory,
      boolean adjustmentBytesInMemoryFlag,
      int adjustmentBytesInMemoryMaxRollupRows,
      int adjustmentBytesInMemoryMaxTimeMs
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, concurrentEventAdd);
    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory == 0 ? Long.MAX_VALUE : maxBytesInMemory;
    this.adjustmentBytesInMemoryFlag = adjustmentBytesInMemoryFlag;
    this.adjustmentBytesInMemoryMaxRollupRows = adjustmentBytesInMemoryMaxRollupRows;
    this.adjustmentBytesInMemoryMaxTimeMs = adjustmentBytesInMemoryMaxTimeMs;
    int syncCount = 0;
    int asyncCount = 0;
    int index = -1;
    int[] syncIndex = new int[this.getMetricAggs().length];
    int[] asyncIndex = new int[this.getMetricAggs().length];
    for (AggregatorFactory metric : this.getMetricAggs()) {
      index++;
      final MaxIntermediateSizeAdjustStrategy maxIntermediateSizeAdjustStrategy = metric
          .getMaxIntermediateSizeAdjustStrategy(adjustmentBytesInMemoryFlag);
      if (maxIntermediateSizeAdjustStrategy == null) {
        continue;
      }
      if (maxIntermediateSizeAdjustStrategy.isSyncAjust()) {
        syncIndex[syncCount++] = index;
      } else {
        asyncIndex[asyncCount++] = index;
      }
    }
    rowNeedSyncAdjustAggIndex = new int[syncCount];
    System.arraycopy(syncIndex, 0, rowNeedSyncAdjustAggIndex, 0, syncCount);

    rowNeedAsyncAdjustAggIndex = new int[asyncCount];
    System.arraycopy(asyncIndex, 0, rowNeedAsyncAdjustAggIndex, 0, asyncCount);

    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts, dimsComparator());
    maxBytesPerRowForAggregators = getMaxBytesPerRowForAggregators(incrementalIndexSchema, adjustmentBytesInMemoryFlag, canAdjust());

    if (existsAsyncAdjust()) {
      // use a blocking single threaded executor to throttle the firehose when adjust bytes size in memory is slow
      final int maxAdjustingThreadNum = 2;
      adjustExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded("[" + this.hashCode() + "]-incrementalindex-adjust", maxAdjustingThreadNum)
      );
    }
  }

  public boolean canAdjust()
  {
    return adjustmentBytesInMemoryFlag
        && maxBytesInMemory != Long.MAX_VALUE
        && (rowNeedAsyncAdjustAggIndex.length > 0 || rowNeedSyncAdjustAggIndex.length > 0);
  }

  public boolean existsAsyncAdjust()
  {
    return canAdjust() && rowNeedAsyncAdjustAggIndex.length > 0;
  }

  public boolean existsSyncAdjust()
  {
    return canAdjust() && rowNeedSyncAdjustAggIndex.length > 0;
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
   * @param canAdjust              whether can adjust bytes
   * @return long max aggregator size in bytes
   */
  private static long getMaxBytesPerRowForAggregators(IncrementalIndexSchema incrementalIndexSchema, final boolean adjustmentBytesInMemoryFlag, boolean canAdjust)
  {
    long maxAggregatorIntermediateSize = ((long) Integer.BYTES) * incrementalIndexSchema.getMetrics().length;
    maxAggregatorIntermediateSize += Arrays.stream(incrementalIndexSchema.getMetrics())
        .mapToLong(aggregator -> {
          if (aggregator.getMaxIntermediateSizeAdjustStrategy(adjustmentBytesInMemoryFlag) != null && canAdjust) {
            final MaxIntermediateSizeAdjustStrategy maxIntermediateSizeAdjustStrategy = aggregator.getMaxIntermediateSizeAdjustStrategy(adjustmentBytesInMemoryFlag);
            return aggregator.getMaxIntermediateSizeWithNulls() + maxIntermediateSizeAdjustStrategy.initAppendBytes()
                + Long.BYTES * 2L;
          }
          return aggregator.getMaxIntermediateSizeWithNulls()
              + Long.BYTES * 2L;
        })
        .sum();
    return maxAggregatorIntermediateSize;
  }

  private void startAsyncAdjust()
  {
    adjustExecutor.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              asyncAdjustBytes();
            }
            catch (Exception e) {
              log.error(e, "Uncaught method[asyncAdjustBytes] exception:");
            }
          }

          private int asyncAdjustBytes()
          {
            long startTime = System.currentTimeMillis();
            List<Integer> tempReadyAdjustList;
            int totalIndexSize = 0;
            int totalNeedAppendBytes = 0;
            while ((tempReadyAdjustList = indexReadyListQueue.poll()) != null) {
              final HashSet<Integer> distinctIndex;
              final int indexSize = tempReadyAdjustList.size();
              distinctIndex = new HashSet(tempReadyAdjustList);
              final Iterator<Integer> iterator = distinctIndex.iterator();
              while (iterator.hasNext()) {
                final Integer index = iterator.next();
                totalNeedAppendBytes += appendBytesInMemoryByAsyncAdjust(
                    index,
                    rowNeedAsyncAdjustAggIndex
                );
              }
              totalIndexSize += indexSize;
            }
            atomicCurrentNeedAppendBytes.addAndGet(totalNeedAppendBytes);
            if (log.isDebugEnabled() && atomicCurrentNeedAppendBytes.get() > 0) {
              log.debug(
                  "Rollup rows:[%s],currentBytesInMemory:[%s],currentNeedAppendBytes:[%s],cost:[%s]",
                  rowNeedAsyncAdjustAggIndex.length,
                  getBytesInMemory().get(),
                  atomicCurrentNeedAppendBytes.get(),
                  System.currentTimeMillis() - startTime
              );
            }
            return totalIndexSize;
          }
        });
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
        log.debug("Aggregators maybe concurrent changed,index:[%s],aggregators.size[%s],metrics:%s",
            index, aggregators.size(), Arrays.toString(metrics)
        );
        continue;
      }
      final MaxIntermediateSizeAdjustStrategy strategy = metrics[rowNeedAdjustAggIndex[ai]]
          .getMaxIntermediateSizeAdjustStrategy(adjustmentBytesInMemoryFlag);
      if (strategy.isSyncAjust()) {
        continue;
      }
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
          if (log.isDebugEnabled()) {
            log.debug(
                "Current bytes[%s] need add adjustAppendSizes[%s],because curAggCardinalRows[%s] reached adjustRollupNums[%s]",
                getBytesInMemory().get(), adjustAppendSizes[i], curAggCardinalRows, adjustRollupNums[i]);
          }
          rowAdjustedCount[ai]++;
          break;
        }

        // when curAggCardinalRows great then adjustRollupNums handle
        if (curAggCardinalRows > adjustRollupNums[i] && rowAdjustedCount[ai] < i + 1) {
          if (log.isDebugEnabled()) {
            log.debug(
                "Current bytes[%s] need add adjustAppendSizes[%s],because index[%s] curAggCardinalRows[%s] exceed adjustRollupNums[%s] but rowAdjustedCount[%s]:%s < needAdjust[%s]",
                getBytesInMemory().get(), adjustAppendSizes[i], index, curAggCardinalRows, adjustRollupNums[i], ai, rowAdjustedCount[ai], i + 1);
          }
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
    final MaxIntermediateSizeAdjustStrategy strategy = metric.getMaxIntermediateSizeAdjustStrategy(
        adjustmentBytesInMemoryFlag);
    if (!strategy.isSyncAjust()) {
      return;
    }
    final int[] adjustRollupNums = strategy.adjustWithRollupNum();
    final int[] adjustAppendSizes = strategy.appendBytesOnRollupNum();
    final int curAggCardinalRows = agg.getCardinalRows(); // maybe main cost method
    for (int i = 0; i < adjustRollupNums.length; i++) {
      if (curAggCardinalRows < adjustRollupNums[i]) { // adjustRollupNums need sort by asc
        break;
      }
      if (curAggCardinalRows == adjustRollupNums[i]) { // need adjust: appending bytes[adjustAppendSizes]
        if (log.isDebugEnabled()) {
          log.debug(
              "Current bytes[%s] need add adjustAppendSizes[%s],because curAggCardinalRows[%s] reached adjustRollupNums[%s]",
              getBytesInMemory().get(), adjustAppendSizes[i], curAggCardinalRows, adjustRollupNums[i]);
        }
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

      // check adjust required
      final long currTime = System.currentTimeMillis();
      if (isAdjustmentRequired()) {
        synchronized (indexReadyListQueue) {
          if (!isAdjustmentRequired()) {
            return;
          }
          adjustBeforeTime = currTime;
          try {
            indexReadyListQueue.put(indexReadyAdjustRecorder);
            indexReadyAdjustRecorder = Collections.synchronizedList(new ArrayList<>());
          }
          catch (InterruptedException e) {
            log.warn(e, "Add ready adjust index list to queue fail.");
          }
        }
        startAsyncAdjust();
      }
    }
  }

  private boolean isAdjustmentRequired()
  {
    final int rollupRowTotal = indexReadyAdjustRecorder.size();
    final long currTime = System.currentTimeMillis();
    return rollupRowTotal >= adjustmentBytesInMemoryMaxRollupRows
        || (currTime - adjustBeforeTime) > adjustmentBytesInMemoryMaxTimeMs;
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
    final List<String> parseExceptionMessages = new ArrayList<>();
    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;
    final AggregatorFactory[] metrics = getMetrics();
    final AtomicInteger numEntries = getNumEntries();
    final AtomicLong sizeInBytes = getBytesInMemory();
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = concurrentGet(priorIndex);
      doAggregate(metrics, aggs, rowContainer, row, parseExceptionMessages);
      recordAdjustIndex(priorIndex);
    } else {
      aggs = new Aggregator[metrics.length];
      factorizeAggs(metrics, aggs, rowContainer, row);
      doAggregate(metrics, aggs, rowContainer, row, parseExceptionMessages);

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
        parseExceptionMessages.clear();
        aggs = concurrentGet(prev);
        doAggregate(metrics, aggs, rowContainer, row, parseExceptionMessages);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    if (canAdjust()) {
      final long appendBytes = atomicCurrentNeedAppendBytes.getAndSet(0);
      final long sizeInBytesAfter = sizeInBytes.addAndGet(appendBytes);
      return new AddToFactsResult(numEntries.get(), sizeInBytesAfter, parseExceptionMessages);
    }
    return new AddToFactsResult(numEntries.get(), sizeInBytes.get(), parseExceptionMessages);
  }

  @VisibleForTesting
  public int getAdjustBytesInMemoryPeriod()
  {
    return adjustmentBytesInMemoryMaxTimeMs;
  }

  @VisibleForTesting
  public int[] getRowNeedAsyncAdjustAggIndex()
  {
    return rowNeedAsyncAdjustAggIndex;
  }

  @VisibleForTesting
  public int[] getRowNeedSyncAdjustAggIndex()
  {
    return rowNeedSyncAdjustAggIndex;
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

  private void doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row,
      List<String> parseExceptionsHolder
  )
  {
    rowContainer.set(row);

    for (int i = 0; i < aggs.length; i++) {
      final Aggregator agg = aggs[i];
      synchronized (agg) {
        try {
          agg.aggregate();
          // sync adjust
          if (existsSyncAdjust()) {
            appendBytesInMemoryBySyncAdjust(agg, metrics[i]);
          }
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          parseExceptionsHolder.add(e.getMessage());
        }
      }
    }

    rowContainer.set(null);
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
    indexReadyAdjustRecorder.clear();
    indexAdjustedRecorder.clear();
  }

  @Override
  public void stopAdjust()
  {
    if (!canAdjust()) {
      return;
    }
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
