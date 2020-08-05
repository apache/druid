package org.apache.druid.query.aggregation.datasketches.theta;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.druid.query.aggregation.datasketches.theta.ThetaSketchSizeAdjustStrategyBenchmark.REPEAT_TIMES;
import static org.apache.druid.query.aggregation.datasketches.theta.ThetaSketchSizeAdjustStrategyBenchmark.WARM_UP_ROUNDS;
import static org.apache.druid.segment.incremental.OnheapIncrementalIndex.ADJUST_BYTES_INMEMORY_FLAG;

@BenchmarkOptions(benchmarkRounds = REPEAT_TIMES, callgc = false, warmupRounds = WARM_UP_ROUNDS)
public class ThetaSketchSizeAdjustStrategyBenchmark extends InitializedNullHandlingTest {
  public static final int WARM_UP_ROUNDS = 2;
  public static final int REPEAT_TIMES = 20;
  private static final int MAX_ROWS = 20_0000;
  private OnheapIncrementalIndex index;
  private int dimCardinalNum;
  private int thetaCardinalNum;
  private Random random = ThreadLocalRandom.current();

  @Before
  public void setUp() {
    SketchModule.registerSerde();
    // dim cardinal
    dimCardinalNum = 2000;
    thetaCardinalNum = 200000;
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
        .buildOnheap();
  }


  @Test
  public void testAdjustThetaSketchNotAdjust() {
    try {
      System.setProperty(ADJUST_BYTES_INMEMORY_FLAG, "false");
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
      }
      index.stopAdjust();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAdjustThetaSketchAdjust() {
    try {
      System.setProperty(ADJUST_BYTES_INMEMORY_FLAG, "true");
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
      }
      index.stopAdjust();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
