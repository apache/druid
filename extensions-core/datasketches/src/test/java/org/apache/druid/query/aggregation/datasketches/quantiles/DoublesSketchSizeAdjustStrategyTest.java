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


package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
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

public class DoublesSketchSizeAdjustStrategyTest extends InitializedNullHandlingTest
{
  private static final int K_SIZE = 1024;
  private static final int MAX_ROWS = 100000;
  private static final long MAX_BYTES = 100_000_000_000L;
  private final Random random = ThreadLocalRandom.current();

  @Before
  public void setup()
  {
    SketchModule.registerSerde();
    DoublesSketchModule.registerSerde();
  }

  @Test
  public void testCreateAdjustStrategy()
  {
    for (int k = 16; k < 32768; k *= 2) {
      AggregatorFactory factory = new DoublesSketchAggregatorFactory("name", "name", k);
      DoublesSketchSizeAdjustStrategy ds = new DoublesSketchSizeAdjustStrategy(k, factory.getMaxIntermediateSize());

      final int estimateMaxSize = Arrays.stream(Arrays.copyOfRange
          (ds.appendBytesOnRollupNum(), 0, ds.appendBytesOnRollupNum().length)).sum();

      final long n = Arrays.stream(Arrays.copyOfRange
          (ds.adjustWithRollupNum(), 0, ds.adjustWithRollupNum().length))
          .sum() * 2;
      final int actualMaxBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
      // Assert.assertEquals(actualMaxBytes, estimateMaxSize);
      System.out.println(actualMaxBytes + "," + estimateMaxSize);
    }
  }

  @Test
  public void testOccupyBytesByAdjustOnCardinal()
  {
    AggregatorFactory factory = new DoublesSketchAggregatorFactory("name", "name", K_SIZE);
    MaxIntermediateSizeAdjustStrategy strategy = new DoublesSketchSizeAdjustStrategy(
        K_SIZE,
        factory.getMaxIntermediateSize()
    );

    final int[] cardinalNums = strategy.adjustWithRollupNum();
    final int[] appendBytesOnCardinalNum = strategy.appendBytesOnRollupNum();
    for (int k = 0; k < cardinalNums.length / 2; k++) {
      int len = cardinalNums[k];
      double[] values = new double[len];
      for (int i = 0; i < len; i++) {
        values[i] = 1;
      }
      final TestDoubleColumnSelectorImpl selector = new TestDoubleColumnSelectorImpl(values);
      final Aggregator agg = new DoublesSketchBuildAggregator(selector, K_SIZE);
      UpdateDoublesSketch sketch;
      GraphLayout graphLayout;
      for (int i = 0; i < len; i++) {
        agg.aggregate();
        selector.increment();
      }

      sketch = (UpdateDoublesSketch) agg.get();
      graphLayout = GraphLayout.parseInstance(sketch);
      final long actualBytes = graphLayout.totalSize();

      final long estimateBytes = factory.getMaxIntermediateSize()
          + strategy.initAppendBytes()
          + Arrays.stream(Arrays.copyOfRange
          (appendBytesOnCardinalNum, 0, k + 1)).sum();

      Assert.assertEquals((int) (estimateBytes / actualBytes), 1, 1);
    }
  }

  @Test
  public void testAppendBytesInMemoryOnRollupCardinal() throws IndexSizeExceededException
  {
    int maxSize = 10000;
    AggregatorFactory quantFactory = new DoublesSketchAggregatorFactory("doublesSketch1", "doublesSketch1", K_SIZE);
    MaxIntermediateSizeAdjustStrategy quantStrategy = new DoublesSketchSizeAdjustStrategy(K_SIZE, quantFactory.getMaxIntermediateSize());

    int[] rollupCardinals = quantStrategy.adjustWithRollupNum();
    int[] quantAppendBytes = quantStrategy.appendBytesOnRollupNum();
    AggregatorFactory[] metrics = {
        quantFactory,
        new DoublesSketchAggregatorFactory("doublesSketch2", "doublesSketch2", K_SIZE)
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
      if (maxSize < rollupCardinals[i]) {
        return;
      }
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

      final long initBytes = notAdjustIndex.getBytesInMemory().get() + quantStrategy.initAppendBytes() * metrics.length;
      final long expectedTotalBytes = (Arrays.stream(
          Arrays.copyOfRange(quantAppendBytes, 0, i + 1)).sum()) * metrics.length;
      final long actualTotalBytes = (adjustIndex.getBytesInMemory().get() - initBytes);
      Assert.assertEquals(expectedTotalBytes, actualTotalBytes);
      // System.out.println(expectedTotalBytes + "," + actualTotalBytes + "," + notAdjustIndex.getBytesInMemory().get() + "," + adjustIndex.getBytesInMemory().get() + "," + initBytes);
      notAdjustIndex.close();
      adjustIndex.close();
    }
  }
}
