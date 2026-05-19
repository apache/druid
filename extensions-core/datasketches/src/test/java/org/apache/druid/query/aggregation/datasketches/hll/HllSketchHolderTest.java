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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.junit.Assert;
import org.junit.Test;

public class HllSketchHolderTest
{
  private static final int LG_K = 12;
  private static final int NUM_VALUES = 1000;

  @Test
  public void testMergeSketchWithSketch()
  {
    HllSketchHolder holder1 = makeSketchHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeSketchHolder(NUM_VALUES, NUM_VALUES * 2);

    HllSketchHolder result = holder1.merge(holder2);

    Assert.assertSame(holder1, result);
    assertEstimateWithinBounds(NUM_VALUES * 2, result);
  }

  @Test
  public void testMergeSketchWithUnion()
  {
    HllSketchHolder holder1 = makeSketchHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeUnionHolder(NUM_VALUES, NUM_VALUES * 2);

    HllSketchHolder result = holder1.merge(holder2);

    Assert.assertSame(holder2, result);
    assertEstimateWithinBounds(NUM_VALUES * 2, result);
  }

  @Test
  public void testMergeUnionWithSketch()
  {
    HllSketchHolder holder1 = makeUnionHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeSketchHolder(NUM_VALUES, NUM_VALUES * 2);

    HllSketchHolder result = holder1.merge(holder2);

    Assert.assertSame(holder1, result);
    assertEstimateWithinBounds(NUM_VALUES * 2, result);
  }

  @Test
  public void testMergeUnionWithUnion()
  {
    HllSketchHolder holder1 = makeUnionHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeUnionHolder(NUM_VALUES, NUM_VALUES * 2);

    HllSketchHolder result = holder1.merge(holder2);

    Assert.assertSame(holder1, result);
    assertEstimateWithinBounds(NUM_VALUES * 2, result);
  }

  @Test
  public void testMergeUnionWithUnionMatchesNaiveMerge()
  {
    HllSketchHolder holder1 = makeUnionHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeUnionHolder(NUM_VALUES, NUM_VALUES * 2);

    HllSketchHolder naiveHolder1 = makeUnionHolder(0, NUM_VALUES);
    HllSketchHolder naiveHolder2 = makeUnionHolder(NUM_VALUES, NUM_VALUES * 2);
    naiveHolder1.add(naiveHolder2.getSketch());

    HllSketchHolder optimizedResult = holder1.merge(holder2);

    Assert.assertEquals(naiveHolder1.getEstimate(), optimizedResult.getEstimate(), 0.0);
  }

  @Test
  public void testMergeUnionWithUnionPreservesTargetType()
  {
    HllSketchHolder holder1 = makeUnionHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeUnionHolder(NUM_VALUES, NUM_VALUES * 2);

    HllSketchHolder result = holder1.merge(holder2);

    Assert.assertEquals(TgtHllType.HLL_4, result.getSketch().getTgtHllType());
  }

  @Test
  public void testMergeWithOverlappingValues()
  {
    HllSketchHolder holder1 = makeUnionHolder(0, NUM_VALUES);
    HllSketchHolder holder2 = makeUnionHolder(NUM_VALUES / 2, NUM_VALUES + NUM_VALUES / 2);

    HllSketchHolder result = holder1.merge(holder2);

    double estimate = result.getEstimate();
    double expected = NUM_VALUES + NUM_VALUES / 2;
    Assert.assertTrue(
        "Estimate " + estimate + " not within 10% of " + expected,
        Math.abs(estimate - expected) / expected < 0.10
    );
  }

  private static HllSketchHolder makeSketchHolder(int startValue, int endValue)
  {
    HllSketch sketch = new HllSketch(LG_K);
    for (int i = startValue; i < endValue; i++) {
      sketch.update(i);
    }
    return HllSketchHolder.of(sketch);
  }

  private static HllSketchHolder makeUnionHolder(int startValue, int endValue)
  {
    HllSketch sketch = new HllSketch(LG_K);
    for (int i = startValue; i < endValue; i++) {
      sketch.update(i);
    }
    Union union = new Union(LG_K);
    union.update(sketch);
    return HllSketchHolder.of(union);
  }

  private static void assertEstimateWithinBounds(double expected, HllSketchHolder holder)
  {
    double estimate = holder.getEstimate();
    Assert.assertTrue(
        "Estimate " + estimate + " not within 10% of " + expected,
        Math.abs(estimate - expected) / expected < 0.10
    );
  }
}
