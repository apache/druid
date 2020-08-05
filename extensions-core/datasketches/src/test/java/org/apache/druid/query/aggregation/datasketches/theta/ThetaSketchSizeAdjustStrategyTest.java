package org.apache.druid.query.aggregation.datasketches.theta;

import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;

import java.util.Arrays;
import java.util.Collections;

public class ThetaSketchSizeAdjustStrategyTest extends InitializedNullHandlingTest {
  @Before
  public void testBefore() {
    SketchModule.registerSerde();
  }

  @Test
  public void testOccupyBytesOnCardinal() {
    // sketch aggregate
    int size = 4096;
    ThetaSketchSizeAdjustStrategy strategy = new ThetaSketchSizeAdjustStrategy(size);
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
      final long estimateBytes = maxUnionBytes + strategy.initAppendBytes() + appendBytesOnCardinalNum[k];
      Assert.assertEquals(actualBytes, estimateBytes, (int) (estimateBytes / 5));
    }
  }

  /**
   * example when size=1024
   * adjust process: adjustBytesInMemoryPositive[false]
   *
   * current cardinal
   *  (need exec adjust)    actual occupy bytes  -   adjust before bytes  =  append bytes
   *        1                   256                      16416                -16160
   *        16                  2052                       256                1796
   *        128                 16416                      2052               14364
   */
  @Test
  public void testAppendBytesInMemoryOnRollupCardinal() throws IndexSizeExceededException, InterruptedException {
    final int size = 1024;
    ThetaSketchSizeAdjustStrategy strategy = new ThetaSketchSizeAdjustStrategy(size);

    int[] rollupCardinals = strategy.adjustWithRollupNum();
    int[] appendBytes = strategy.appendBytesOnRollupNum();
    for (int i = 0; i < rollupCardinals.length; i++) {
      final int cardinalIndex = i;
      OnheapIncrementalIndex index = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(
                      new SketchMergeAggregatorFactory("theta01", "theta01",
                          size, null, null, null))
                  .build()
          )
          .setMaxRowCount(10_0000)
          .buildOnheap();
      OnheapIncrementalIndex index2 = (OnheapIncrementalIndex) new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withQueryGranularity(Granularities.MINUTE)
                  .withMetrics(
                      new SketchMergeAggregatorFactory("theta01", "theta01",
                          size, null, null, null))
                  .build()
          )
          .setMaxRowCount(10_0000)
          .buildOnheap();

      for (int j = 0; j < rollupCardinals[cardinalIndex]; ++j) {
        index.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", 1)
        ));
        index2.add(new MapBasedInputRow(
            0,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", 1,
                "theta01", j)
        ));
      }
      Thread.sleep(index.getAdjustBytesInMemoryPeriod() * 2);
      index2.add(new MapBasedInputRow(
          0,
          Collections.singletonList("dim1"),
          ImmutableMap.of("dim1", 1,
              "theta01", 11111)
      ));
      Assert.assertEquals(index2.getBytesInMemory().get() - index.getBytesInMemory().get(), appendBytes[cardinalIndex]);
    }
  }
}
