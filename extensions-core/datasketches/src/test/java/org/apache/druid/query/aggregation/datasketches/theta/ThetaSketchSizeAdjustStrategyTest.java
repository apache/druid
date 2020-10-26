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
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
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

  @Before
  public void setup()
  {
    SketchModule.registerSerde();
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
  public void testAppendBytesInMemoryOnRollupCardinal() throws IndexSizeExceededException
  {
    final int size = 16384;
    ThetaSketchSizeAdjustStrategy strategy = new ThetaSketchSizeAdjustStrategy(size);
    int[] rollupCardinals = strategy.adjustWithRollupNum();
    int[] appendBytes = strategy.appendBytesOnRollupNum();
    AggregatorFactory[] metrics = {
        new SketchMergeAggregatorFactory("theta01", "theta01",
            size, null, null, null
        ), new SketchMergeAggregatorFactory("theta02", "theta02",
        size, null, null, null),
        new SketchMergeAggregatorFactory("theta03", "theta03",
            size, null, null, null)
    };
    IncrementalIndex notAdjustIndex;
    IncrementalIndex adjustIndex;
    for (int i = 0; i < rollupCardinals.length; i++) {
      notAdjustIndex = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(metrics)
                  .build()
          )
          .setMaxRowCount(MAX_ROWS)
          .setMaxBytesInMemory(MAX_BYTES)
          .setAdjustmentBytesInMemoryFlag(false)
          .buildOnheap();
      adjustIndex = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(metrics)
                  .build()
          )
          .setMaxRowCount(MAX_ROWS)
          .setMaxBytesInMemory(MAX_BYTES)
          .setAdjustmentBytesInMemoryFlag(true)
          .buildOnheap();
      for (int j = 0; j < rollupCardinals[i]; ++j) {
        String diffVal = random.nextInt(Integer.MAX_VALUE) + "";
        notAdjustIndex.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", 1, "theta02", 1)
        ));

        adjustIndex.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", diffVal, "theta02", diffVal
            )
        ));
      }

      final long initBytes = notAdjustIndex.getBytesInMemory().get() + strategy.initAppendBytes() * metrics.length;
      final long expectedTotalBytes = (Arrays.stream(
          Arrays.copyOfRange(appendBytes, 0, i + 1)).sum()) * metrics.length;
      final long actualTotalBytes = adjustIndex.getBytesInMemory().get() - initBytes;
      Assert.assertEquals(expectedTotalBytes, actualTotalBytes);
      // System.out.println(expectedTotalBytes + "," + actualTotalBytes + "," + notAdjustIndex.getBytesInMemory().get() + "," + adjustIndex.getBytesInMemory().get() + "," + initBytes);
      notAdjustIndex.stopAdjust();
      adjustIndex.stopAdjust();
      notAdjustIndex.close();
      adjustIndex.close();
    }
  }

}
