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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregator;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class OnheapIncrementalIndexTest extends InitializedNullHandlingTest
{
  private static final int MAX_ROWS = 100000;

  private MaxIntermediateSizeAdjustStrategy customAggStrategy = null;
  private Aggregator customAgg = null;
  private AggregatorFactory customMetric = null;

  public void initCustomAggAdjustStrategy(boolean isSyncAggAdjust)
  {
    customAggStrategy = new MaxIntermediateSizeAdjustStrategy()
    {
      final int[] rollupNums = {2, 20, 40, 400};
      final int[] appendBytesWithNum = {1000, 1000, 1000, 1000};

      @Override
      public boolean isSyncAjust()
      {
        return isSyncAggAdjust;
      }

      @Override
      public int[] adjustWithRollupNum()
      {
        return rollupNums;
      }

      @Override
      public int[] appendBytesOnRollupNum()
      {
        return appendBytesWithNum;
      }

      @Override
      public int initAppendBytes()
      {
        return 100;
      }
    };

    customAgg = new Aggregator()
    {
      AtomicInteger occupy = new AtomicInteger(0);

      @Override
      public void aggregate()
      {
        occupy.incrementAndGet();
      }

      @Override
      public int getCardinalRows()
      {
        if (!isSyncAggAdjust) {
          try {
            Thread.sleep(1);
          }
          catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        return (int) get();
      }

      @Nullable
      @Override
      public Object get()
      {
        return occupy.get();
      }

      @Override
      public float getFloat()
      {
        return occupy.get();
      }

      @Override
      public long getLong()
      {
        return occupy.get();
      }

      @Override
      public void close()
      {
        occupy = new AtomicInteger(0);
      }
    };

    customMetric = new AggregatorFactory()
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        return customAgg;
      }

      @Nullable
      @Override
      public MaxIntermediateSizeAdjustStrategy getMaxIntermediateSizeAdjustStrategy()
      {
        return customAggStrategy;
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        return null;
      }

      @Override
      public Comparator getComparator()
      {
        return null;
      }

      @Nullable
      @Override
      public Object combine(@Nullable Object lhs, @Nullable Object rhs)
      {
        return null;
      }

      @Override
      public AggregatorFactory getCombiningFactory()
      {
        return null;
      }

      @Override
      public List<AggregatorFactory> getRequiredColumns()
      {
        return null;
      }

      @Override
      public Object deserialize(Object object)
      {
        return null;
      }

      @Nullable
      @Override
      public Object finalizeComputation(@Nullable Object object)
      {
        return null;
      }

      @Override
      public String getName()
      {
        return "custom";
      }

      @Override
      public List<String> requiredFields()
      {
        return new ArrayList<>();
      }

      @Override
      public ValueType getType()
      {
        return ValueType.COMPLEX;
      }

      @Override
      public ValueType getFinalizedType()
      {
        return ValueType.COMPLEX;
      }

      @Override
      public String getComplexTypeName()
      {
        return "custom";
      }

      @Override
      public int getMaxIntermediateSize()
      {
        return 4;
      }

      @Override
      public byte[] getCacheKey()
      {
        return new byte[0];
      }
    };

    ComplexMetrics.registerSerde("custom", new ComplexMetricSerde()
    {
      @Override
      public String getTypeName()
      {
        return "custom";
      }

      @Override
      public ComplexMetricExtractor getExtractor()
      {
        return new ComplexMetricExtractor()
        {
          @Override
          public Class extractedClass()
          {
            return customMetric.getClass();
          }

          @Nullable
          @Override
          public Object extractValue(InputRow inputRow, String metricName)
          {
            return inputRow.getMetric(metricName);
          }
        };
      }

      @Override
      public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
      {

      }

      @Override
      public ObjectStrategy getObjectStrategy()
      {
        return null;
      }
    });
  }

  @Test
  public void testCustomAggAsyncAdjustStrategy() throws IndexSizeExceededException, InterruptedException
  {
    initCustomAggAdjustStrategy(false);
    final AggregatorFactory[] metrics = {new LongSumAggregatorFactory("sum1", "sum1"),
        new LongMinAggregatorFactory("min1", "min1"), customMetric};
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG, "false");
    final OnheapIncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();
    IncrementalIndexAddResult addResult1 = null;
    for (int i = 0; i < MAX_ROWS; i++) {
      addResult1 = index.add(new MapBasedInputRow(
          0,
          Collections.singletonList("billy"),
          ImmutableMap.of("billy", 1, "sum1", i, "min1", i, "custom", 1)
      ));
    }
    final long notAdjustBytes = addResult1.getBytesInMemory();
    index.stopAdjust();
    index.close();

    initCustomAggAdjustStrategy(false);
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG, "true");
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_PERIOD, "5");
    final OnheapIncrementalIndex indexAdjust = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();
    Thread.sleep(indexAdjust.adjustBytesInMemoryPeriod);

    final int addThreadCount = 2;
    Thread[] addThreads = new Thread[addThreadCount];
    final CountDownLatch downLatch = new CountDownLatch(addThreadCount);
    for (int t = 0; t < addThreadCount; ++t) {
      addThreads[t] = new Thread(() -> {
        try {
          for (int i = 0; i < MAX_ROWS / addThreadCount; i++) {
            indexAdjust.add(new MapBasedInputRow(
                0,
                Collections.singletonList("billy"),
                ImmutableMap.of("billy", 1, "sum1", i, "min1", i, "custom", 1)
            ));

          }
          downLatch.countDown();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      addThreads[t].start();
    }
    downLatch.await();
    Thread.sleep(indexAdjust.adjustBytesInMemoryPeriod);
    indexAdjust.add(new MapBasedInputRow(
        0,
        Collections.singletonList("billy"),
        ImmutableMap.of("billy", 1, "sum1", 1, "min1", 1, "custom", 1)
    ));

    final long adjustBytes = indexAdjust.getBytesInMemory().get();
    final long actualAppendBytes = adjustBytes - notAdjustBytes;
    final int[] appendBytess = customAggStrategy.appendBytesOnRollupNum();

    indexAdjust.stopAdjust();
    indexAdjust.close();
    Assert.assertEquals(Arrays.stream(appendBytess).sum() + customAggStrategy.initAppendBytes(), actualAppendBytes);
    Assert.assertEquals(true, indexAdjust.rowNeedAsyncAdjustAggIndex.length == 1 && indexAdjust.rowNeedAsyncAdjustAggIndex[0] == 2);
  }

  @Test
  public void testCustomAggSyncAdjustStrategy() throws IndexSizeExceededException, InterruptedException
  {
    initCustomAggAdjustStrategy(true);
    final AggregatorFactory[] metrics = {new LongSumAggregatorFactory("sum1", "sum1"),
        new LongMinAggregatorFactory("min1", "min1"), customMetric};
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG, "false");
    final OnheapIncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();
    IncrementalIndexAddResult addResult1 = null;
    for (int i = 0; i < MAX_ROWS; i++) {
      addResult1 = index.add(new MapBasedInputRow(
          0,
          Collections.singletonList("billy"),
          ImmutableMap.of("billy", 1, "sum1", i, "min1", i, "custom", 1)
      ));
    }
    final long notAdjustBytes = addResult1.getBytesInMemory();
    index.stopAdjust();

    initCustomAggAdjustStrategy(true);
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG, "true");
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_PERIOD, "5");
    final OnheapIncrementalIndex indexAdjust = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();

    final int addThreadCount = 2;
    Thread[] addThreads = new Thread[addThreadCount];
    final CountDownLatch downLatch = new CountDownLatch(addThreadCount);
    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i] = new Thread(() -> {
        try {
          for (int i1 = 0; i1 < MAX_ROWS / addThreadCount; i1++) {
            indexAdjust.add(new MapBasedInputRow(
                0,
                Collections.singletonList("billy"),
                ImmutableMap.of("billy", 1, "sum1", i1, "min1", i1, "custom", 1)
            ));
          }
          downLatch.countDown();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      addThreads[i].start();
    }
    downLatch.await();
    final IncrementalIndexAddResult addResult = indexAdjust.add(new MapBasedInputRow(
        0,
        Collections.singletonList("billy"),
        ImmutableMap.of("billy", 1, "sum1", 1, "min1", 1, "custom", 1)
    ));
    final long adjustBytes = addResult.getBytesInMemory();
    indexAdjust.stopAdjust();
    indexAdjust.close();

    final long actualAppendBytes = adjustBytes - notAdjustBytes;
    final int[] appendBytess = customAggStrategy.appendBytesOnRollupNum();
    Assert.assertEquals(Arrays.stream(appendBytess).sum() + customAggStrategy.initAppendBytes(), actualAppendBytes);
    Assert.assertEquals(true, indexAdjust.rowNeedSyncAdjustAggIndex.length == 1 && indexAdjust.rowNeedSyncAdjustAggIndex[0] == 2);
  }

  @Test
  public void testMultithreadAddFacts() throws Exception
  {
    final IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(new LongMaxAggregatorFactory("max", "max"))
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();

    final int addThreadCount = 2;
    Thread[] addThreads = new Thread[addThreadCount];
    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i] = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          final Random random = ThreadLocalRandom.current();
          try {
            for (int j = 0; j < MAX_ROWS / addThreadCount; ++j) {
              index.add(new MapBasedInputRow(
                  0,
                  Collections.singletonList("billy"),
                  ImmutableMap.of("billy", random.nextLong(), "max", 1)
              ));
            }
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
      addThreads[i].start();
    }

    final AtomicInteger checkFailedCount = new AtomicInteger(0);
    Thread checkThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        while (!Thread.interrupted()) {
          for (IncrementalIndexRow row : index.getFacts().keySet()) {
            if (index.getMetricLongValue(row.getRowIndex(), 0) != 1) {
              checkFailedCount.addAndGet(1);
            }
          }
        }
      }
    });
    checkThread.start();

    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i].join();
    }
    checkThread.interrupt();

    Assert.assertEquals(0, checkFailedCount.get());
  }

  @Test
  public void testMultithreadAddFactsUsingExpressionAndJavaScript() throws Exception
  {
    final IncrementalIndex indexExpr = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(new LongSumAggregatorFactory(
                    "oddnum",
                    null,
                    "if(value%2==1,1,0)",
                    TestExprMacroTable.INSTANCE
                ))
                .withRollup(true)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();

    final IncrementalIndex indexJs = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(new JavaScriptAggregatorFactory(
                    "oddnum",
                    ImmutableList.of("value"),
                    "function(current, value) { if (value%2==1) current = current + 1; return current;}",
                    "function() {return 0;}",
                    "function(a, b) { return a + b;}",
                    JavaScriptConfig.getEnabledInstance()
                ))
                .withRollup(true)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();

    final int addThreadCount = 2;
    Thread[] addThreads = new Thread[addThreadCount];
    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i] = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          final Random random = ThreadLocalRandom.current();
          try {
            for (int j = 0; j < MAX_ROWS / addThreadCount; ++j) {
              int randomInt = random.nextInt(100000);
              MapBasedInputRow mapBasedInputRowExpr = new MapBasedInputRow(
                  0,
                  Collections.singletonList("billy"),
                  ImmutableMap.of("billy", randomInt % 3, "value", randomInt)
              );
              MapBasedInputRow mapBasedInputRowJs = new MapBasedInputRow(
                  0,
                  Collections.singletonList("billy"),
                  ImmutableMap.of("billy", randomInt % 3, "value", randomInt)
              );
              indexExpr.add(mapBasedInputRowExpr);
              indexJs.add(mapBasedInputRowJs);
            }
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
      addThreads[i].start();
    }

    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i].join();
    }

    long exprSum = 0;
    long jsSum = 0;

    for (IncrementalIndexRow row : indexExpr.getFacts().keySet()) {
      exprSum += indexExpr.getMetricLongValue(row.getRowIndex(), 0);
    }

    for (IncrementalIndexRow row : indexJs.getFacts().keySet()) {
      jsSum += indexJs.getMetricLongValue(row.getRowIndex(), 0);
    }

    Assert.assertEquals(exprSum, jsSum);
  }

  @Test
  public void testOnHeapIncrementalIndexClose() throws Exception
  {
    // Prepare the mocks & set close() call count expectation to 1
    Aggregator mockedAggregator = EasyMock.createMock(LongMaxAggregator.class);
    mockedAggregator.close();
    EasyMock.expectLastCall().times(1);

    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(new LongMaxAggregatorFactory("max", "max"))
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();

    index.add(new MapBasedInputRow(
            0,
            Collections.singletonList("billy"),
            ImmutableMap.of("billy", 1, "max", 1)
    ));

    // override the aggregators with the mocks
    index.concurrentGet(0)[0] = mockedAggregator;

    // close the indexer and validate the expectations
    EasyMock.replay(mockedAggregator);
    index.close();
    EasyMock.verify(mockedAggregator);
  }
}
