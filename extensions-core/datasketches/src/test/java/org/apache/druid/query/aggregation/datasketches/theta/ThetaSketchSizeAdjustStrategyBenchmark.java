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

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@BenchmarkOptions(benchmarkRounds = ThetaSketchSizeAdjustStrategyBenchmark.REPEAT_TIMES, warmupRounds = ThetaSketchSizeAdjustStrategyBenchmark.WARM_UP_ROUNDS)
public class ThetaSketchSizeAdjustStrategyBenchmark extends InitializedNullHandlingTest
{
  public static final int WARM_UP_ROUNDS = 2;
  public static final int REPEAT_TIMES = 20;
  private static final int MAX_ROWS = 20_0000;
  private static final int MAX_BYTES = 100_000_000;
  private OnheapIncrementalIndex index;
  private int dimCardinalNum;
  private int thetaCardinalNum;
  private Random random = ThreadLocalRandom.current();

  @Before
  public void setUp()
  {
    SketchModule.registerSerde();
    // dim cardinal
    dimCardinalNum = 2000;
    thetaCardinalNum = 200000;

    int periodMills = random.nextInt(5) * 1000;
    System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_PERIOD, periodMills + "");
  }


  @Test
  public void testAdjustThetaSketchNotAdjust()
  {
    try {
      System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG, "false");
      index = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(
                      new LongMinAggregatorFactory("longmin01", "longmin01"),
                      new SketchMergeAggregatorFactory("theta01", "theta01",
                          1024, null, null, null),
                      new LongMaxAggregatorFactory("longmax01", "longmax01"))
                  .build()
          )
          .setMaxRowCount(MAX_ROWS)
          .setMaxBytesInMemory(MAX_BYTES)
          .buildOnheap();
      for (int j = 0; j < MAX_ROWS; ++j) {
        int dim = random.nextInt(dimCardinalNum);
        index.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", dim,
                "theta01", random.nextInt(thetaCardinalNum),
                "longmin01", random.nextInt(100),
                "longmax01", random.nextInt(100))
        ));
        Assert.assertEquals(false, index.canAdjust());
      }
      index.stopAdjust();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAdjustThetaSketchAdjust()
  {
    try {
      System.setProperty(OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG, "true");
      index = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(
                      new LongMinAggregatorFactory("longmin01", "longmin01"),
                      new SketchMergeAggregatorFactory("theta01", "theta01",
                          1024, null, null, null),
                      new LongMaxAggregatorFactory("longmax01", "longmax01"))
                  .build()
          )
          .setMaxRowCount(MAX_ROWS)
          .setMaxBytesInMemory(MAX_BYTES)
          .buildOnheap();
      for (int j = 0; j < MAX_ROWS; ++j) {
        int dim = random.nextInt(dimCardinalNum);
        final IncrementalIndexAddResult addResult = index.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", dim,
                "theta01", random.nextInt(thetaCardinalNum),
                "longmin01", random.nextInt(100),
                "longmax01", random.nextInt(100))
        ));
        Assert.assertEquals(true, index.canAdjust());
      }
      index.stopAdjust();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
