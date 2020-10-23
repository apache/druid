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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class ThetaSketchSizeAdjustStrategyTest extends InitializedNullHandlingTest
{
  private static final int MAX_ROWS = 100000;
  private static final long MAX_BYTES = 100_000_000_000L;
  private final Random random = ThreadLocalRandom.current();
  private final boolean adjustFlag = true;
  private final int adjustRollupRows = 100;
  private final int adjustTimeMs = 1000;

  @Before
  public void setup()
  {
    SketchModule.registerSerde();
  }

  @Test
  public void testAdjustStrategyInstanceByPara()
  {
    OnheapIncrementalIndex index = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(new SketchMergeAggregatorFactory("theta01", "theta01",
                    1024, null, null, null))
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .setMaxBytesInMemory(MAX_BYTES)
        .setAdjustmentBytesInMemoryFlag(adjustFlag)
        .setAdjustmentBytesInMemoryMaxRollupRows(adjustRollupRows)
        .setadjustmentBytesInMemoryMaxTimeMs(adjustTimeMs)
        .buildOnheap();
    Assert.assertEquals(true, index.existsAsyncAdjust() && !index.existsSyncAdjust()
        && index.getRowNeedAsyncAdjustAggIndex().length == 1 && index.getRowNeedSyncAdjustAggIndex().length == 0);
  }

  @Test
  public void testThetaSketchSizeAdjustStrategyOnDiffSize()
  {
    MaxIntermediateSizeAdjustStrategy strategy;
    SketchAggregatorFactory aggregatorFactory;
    for (int size = 1; size <= 16384; size *= 2) {
      strategy = new ThetaSketchSizeAdjustStrategy(size);
      aggregatorFactory = new SketchMergeAggregatorFactory("theta01",
          "theta01",
          1024, null, null, null);
      Assert.assertEquals(true, strategy != null && strategy.adjustWithRollupNum().length == strategy
          .appendBytesOnRollupNum().length);

      final long maxBytesPerAgg = aggregatorFactory.getMaxIntermediateSize();
      final long appendBytesTotal = maxBytesPerAgg + strategy.initAppendBytes() + Arrays.stream(strategy
          .appendBytesOnRollupNum()).sum();
      Assert.assertEquals(maxBytesPerAgg, appendBytesTotal);
    }
  }

  @Test
  public void testOccupyBytesByAdjustOnCardinal()
  {
    // sketch aggregate
    int size = 4096;
    MaxIntermediateSizeAdjustStrategy strategy = new ThetaSketchSizeAdjustStrategy(size);
    final int[] cardinalNums = strategy.adjustWithRollupNum();
    final int[] appendBytesOnCardinalNum = strategy.appendBytesOnRollupNum();
    for (int k = 0; k < cardinalNums.length; k++) {
      Union union = (Union) SetOperation.builder().setNominalEntries(size).build(Family.UNION);
      final long maxUnionBytes = new SketchMergeAggregatorFactory("theta01", "theta01",
          size, null, null, null).getMaxIntermediateSizeWithNulls();
      for (int i = 0; i < cardinalNums[k] + 1; i++) {
        union.update(i);
      }
      GraphLayout graphLayout = GraphLayout.parseInstance(union);
      final long actualBytes = graphLayout.totalSize();
      final long estimateBytes = maxUnionBytes + strategy.initAppendBytes() + Arrays.stream(Arrays.copyOfRange
          (appendBytesOnCardinalNum, 0, k + 1)).sum();
      Assert.assertEquals((int) (estimateBytes / actualBytes), 1, 1);
    }
  }

  /**
   * example when size=1024
   * <p>
   * current cardinal
   * (need exec adjust)    actual occupy bytes  -   adjust before bytes  =  append bytes
   * 1                   256                      16416                -16160
   * 16                  2052                       256                1796
   * 128                 16416                      2052               14364
   */
  @Test
  public void testAppendBytesInMemoryOnRollupCardinal() throws IndexSizeExceededException, InterruptedException
  {
    final int size = 16384;
    ThetaSketchSizeAdjustStrategy strategy = new ThetaSketchSizeAdjustStrategy(size);
    int[] rollupCardinals = strategy.adjustWithRollupNum();
    int[] appendBytes = strategy.appendBytesOnRollupNum();
    AggregatorFactory[] metrics = {
        new SketchMergeAggregatorFactory("theta01", "theta01",
            size, null, null, null
        )
    };
    final IncrementalIndex sameIndex = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .setMaxBytesInMemory(MAX_BYTES)
        .setAdjustmentBytesInMemoryFlag(adjustFlag)
        .setAdjustmentBytesInMemoryMaxRollupRows(adjustRollupRows)
        .setadjustmentBytesInMemoryMaxTimeMs(adjustTimeMs)
        .buildOnheap();
    final IncrementalIndex diffIndex = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .setMaxBytesInMemory(MAX_BYTES)
        .setAdjustmentBytesInMemoryFlag(adjustFlag)
        .setAdjustmentBytesInMemoryMaxRollupRows(adjustRollupRows)
        .setadjustmentBytesInMemoryMaxTimeMs(adjustTimeMs)
        .buildOnheap();
    for (int i = 0; i < rollupCardinals.length; i++) {
      final int cardinalIndex = i;

      for (int j = 0; j < rollupCardinals[cardinalIndex] + 2; ++j) {
        String diffVal = random.nextInt(Integer.MAX_VALUE) + "";
        sameIndex.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", 1, "theta02", 1)
        ));

        diffIndex.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", diffVal, "theta02", diffVal
            )
        ));

        if (j >= rollupCardinals[cardinalIndex] - 1) {
          Thread.sleep(adjustTimeMs);
        }
      }
      final long expectedTotalBytes = (Arrays.stream(
          Arrays.copyOfRange(appendBytes, 0, cardinalIndex + 1)).sum()) * metrics.length;
      final long actualTotalBytes = diffIndex.getBytesInMemory().get()
          - sameIndex.getBytesInMemory().get();
      Assert.assertEquals(expectedTotalBytes, actualTotalBytes);
    }
    sameIndex.stopAdjust();
    diffIndex.stopAdjust();
    sameIndex.close();
    diffIndex.close();
  }

  @Test
  public void testNeedAdjustAggIndex()
  {
    int metricNum = 10;
    int[] actualNeedAdjustMetricIndex = {0, 1, 4, 9};
    AggregatorFactory[] metrics = new AggregatorFactory[metricNum];
    for (int i = 0; i < metricNum; i++) {
      boolean flag = false;
      for (int anActualNeedAdjustMetricIndex : actualNeedAdjustMetricIndex) {
        if (i == anActualNeedAdjustMetricIndex) {
          metrics[i] = new SketchMergeAggregatorFactory("theta01" + i, "theta01" + i,
              1024, null, null, null);
          flag = true;
          break;
        }
      }
      if (!flag) {
        metrics[i] = new LongMaxAggregatorFactory("max" + i, "max" + i);
      }
    }

    OnheapIncrementalIndex index = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.MINUTE)
                .withMetrics(metrics)
                .build()
        )
        .setMaxRowCount(MAX_ROWS)
        .setMaxBytesInMemory(MAX_BYTES)
        .setAdjustmentBytesInMemoryFlag(adjustFlag)
        .setAdjustmentBytesInMemoryMaxRollupRows(adjustRollupRows)
        .setadjustmentBytesInMemoryMaxTimeMs(adjustTimeMs)
        .buildOnheap();
    final int[] rowNeedAdjustAggIndex = index.getRowNeedAsyncAdjustAggIndex();
    Assert.assertArrayEquals(actualNeedAdjustMetricIndex, rowNeedAdjustAggIndex);
  }
}
