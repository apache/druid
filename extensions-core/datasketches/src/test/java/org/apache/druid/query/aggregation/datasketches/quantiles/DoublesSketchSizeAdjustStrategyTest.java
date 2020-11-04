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
import org.apache.druid.query.aggregation.CountAdjustmentHolder;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;
import org.apache.druid.query.aggregation.MetricAdjustmentHolder;
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
import java.util.HashMap;
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
    int k = 128;
    AggregatorFactory quantFactory = new DoublesSketchAggregatorFactory("doublesSketch1", "doublesSketch1", k);
    DoublesSketchSizeAdjustStrategy ds = new DoublesSketchSizeAdjustStrategy(k);
    final int[] rollNums = ds.adjustWithRollupNum();
    final int[] appendBytesOnRollupNum = ds.appendBytesOnRollupNum();

    for (int rollIndex = 0; rollIndex < appendBytesOnRollupNum.length; rollIndex++) {
      final int totalAppendingBytes = Arrays.stream(Arrays.copyOfRange
          (appendBytesOnRollupNum, 0, rollIndex)).sum();
      final int n = rollNums[rollIndex];
      final int actualMaxBytes = DoublesSketch.getUpdatableStorageBytes(k, n);
      final long iniAppendEstimate = quantFactory.getMaxIntermediateSize() + ds.initAppendBytes();
      final long estimateBytes = iniAppendEstimate + totalAppendingBytes;
      Assert.assertEquals(actualMaxBytes, estimateBytes);
      // System.out.println(k + "," + n + ", " + actualMaxBytes + "," + totalAppendingBytes + "," +
      //     (totalAppendingBytes + iniAppendEstimate));
    }
  }

  @Test
  public void testOccupyBytesByAdjustOnCardinal()
  {
    AggregatorFactory factory = new DoublesSketchAggregatorFactory("name", "name", K_SIZE);
    MaxIntermediateSizeAdjustStrategy strategy = new DoublesSketchSizeAdjustStrategy(K_SIZE);

    final int[] cardinalNums = strategy.adjustWithRollupNum();
    final int[] appendBytesOnCardinalNum = strategy.appendBytesOnRollupNum();
    for (int k = 0; k < Math.min(4, cardinalNums.length); k++) {
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
          (appendBytesOnCardinalNum, 0, k)).sum();

      Assert.assertEquals((int) (estimateBytes / actualBytes), 1, 1);
    }
  }

  @Test
  public void testAppendBytesInMemoryOnRollupCardinal() throws IndexSizeExceededException
  {
    int maxSize = 10000;
    MaxIntermediateSizeAdjustStrategy quantStrategy = new DoublesSketchSizeAdjustStrategy(K_SIZE);

    int[] rollupCardinals = quantStrategy.adjustWithRollupNum();
    int[] quantAppendBytes = quantStrategy.appendBytesOnRollupNum();
    IncrementalIndex notAdjustIndex;
    IncrementalIndex adjustIndex;
    for (int i = 0; i < rollupCardinals.length; i++) {
      if (maxSize < rollupCardinals[i]) {
        return;
      }
      AggregatorFactory[] metricsNotAdjust = {
          new DoublesSketchAggregatorFactory("doublesSketch1", "doublesSketch1", K_SIZE),
          new DoublesSketchAggregatorFactory("doublesSketch2", "doublesSketch2", K_SIZE)
      };
      notAdjustIndex = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(metricsNotAdjust)
                  .build()
          )
          .setMaxRowCount(MAX_ROWS)
          .setMaxBytesInMemory(MAX_BYTES)
          .buildOnheap();

      for (int j = 0; j < rollupCardinals[i]; ++j) {
        String diffVal = random.nextInt(Integer.MAX_VALUE) + "";
        notAdjustIndex.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", 1, "theta02", 1)
        ));
      }

      AggregatorFactory[] metricsAdjust = {
          new DoublesSketchAggregatorFactory("doublesSketch1", "doublesSketch1", K_SIZE),
          new DoublesSketchAggregatorFactory("doublesSketch2", "doublesSketch2", K_SIZE)
      };
      CountAdjustmentHolder adjustmentHolder = createAdjustmentHolder(metricsAdjust, MAX_BYTES, true);
      adjustIndex = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(metricsAdjust)
                  .build()
          )
          .setMaxRowCount(MAX_ROWS)
          .setMaxBytesInMemory(MAX_BYTES)
          .setAdjustmentHolder(adjustmentHolder)
          .buildOnheap();
      for (int j = 0; j < rollupCardinals[i]; ++j) {
        String diffVal = random.nextInt(Integer.MAX_VALUE) + "";
        adjustIndex.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", diffVal, "theta02", diffVal
            )
        ));
      }

      final long initBytes = notAdjustIndex.getBytesInMemory().get() + quantStrategy.initAppendBytes() * metricsAdjust.length;
      final long expectedTotalBytes = (Arrays.stream(
          Arrays.copyOfRange(quantAppendBytes, 0, i + 1)).sum()) * metricsAdjust.length;
      final long actualTotalBytes = (adjustIndex.getBytesInMemory().get() - initBytes);
      // Assert.assertEquals(expectedTotalBytes, actualTotalBytes);
      System.out.println(expectedTotalBytes + "," + actualTotalBytes + "," + notAdjustIndex.getBytesInMemory().get() + "," + adjustIndex.getBytesInMemory().get() + "," + initBytes);
      notAdjustIndex.close();
      adjustIndex.close();
    }
  }

  private CountAdjustmentHolder createAdjustmentHolder(
      AggregatorFactory[] metrics, long maxBytesInMemory, final boolean
      adjustmentFlag
  )
  {
    HashMap<String, MetricAdjustmentHolder> metricTypeAndHolderMap = new HashMap<>();
    if (maxBytesInMemory < 0 || adjustmentFlag == false) {
      return null;
    }
    for (AggregatorFactory metric : metrics) {
      final MaxIntermediateSizeAdjustStrategy maxIntermediateSizeAdjustStrategy = metric
          .getMaxIntermediateSizeAdjustStrategy(adjustmentFlag);
      if (maxIntermediateSizeAdjustStrategy == null) {
        continue;
      }
      final String tempMetricType = maxIntermediateSizeAdjustStrategy.getAdjustmentMetricType();
      final MetricAdjustmentHolder metricAdjustmentHolder = metricTypeAndHolderMap.computeIfAbsent(
          tempMetricType,
          k -> new MetricAdjustmentHolder(maxIntermediateSizeAdjustStrategy)
      );
      if (metricAdjustmentHolder != null) {
        metricAdjustmentHolder.selectStrategyByType(maxIntermediateSizeAdjustStrategy);
      }
    }
    CountAdjustmentHolder adjustmentHolder = null;
    if (metricTypeAndHolderMap.size() > 0) {
      adjustmentHolder = new CountAdjustmentHolder(metricTypeAndHolderMap);
    }
    return adjustmentHolder;
  }
}
