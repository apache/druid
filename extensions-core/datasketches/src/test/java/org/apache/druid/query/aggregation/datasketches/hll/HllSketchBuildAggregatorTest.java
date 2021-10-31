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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests for {@link HllSketchBuildAggregator#updateSketch}.
 *
 * Tests of the aggregator generally should go in {@link HllSketchAggregatorTest} instead.
 */
public class HllSketchBuildAggregatorTest extends InitializedNullHandlingTest
{
  private final HllSketch sketch = new HllSketch(HllSketch.DEFAULT_LG_K);

  @Test
  public void testUpdateSketchVariousNumbers()
  {
    updateSketch(1L, -2L, 1L, -2, 1L, 2.0, 2f, Double.doubleToLongBits(2.0), 3.0);
    assertSketchEstimate(4);
  }

  @Test
  public void testUpdateSketchStrings()
  {
    updateSketch("foo", null, "bar", "");
    assertSketchEstimate(2);
  }

  @Test
  public void testUpdateSketchListsOfStrings()
  {
    updateSketch(
        Arrays.asList("1", "2"),
        Arrays.asList("2", "", "3", "11"),
        Arrays.asList("1", null, "3", "12"),
        Arrays.asList("1", "3", "13")
    );

    assertSketchEstimate(6);
  }

  @Test
  public void testUpdateSketchCharArray()
  {
    updateSketch(
        new char[]{1, 2},
        new char[]{2, 3, 11},
        new char[]{1, 2},
        new char[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchByteArray()
  {
    updateSketch(
        new byte[]{1, 2},
        new byte[]{2, 3, 11},
        new byte[]{1, 2},
        new byte[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchIntArray()
  {
    updateSketch(
        new int[]{1, 2},
        new int[]{2, 3, 11},
        new int[]{1, 2},
        new int[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchLongArray()
  {
    updateSketch(
        new long[]{1, 2},
        new long[]{2, 3, 11},
        new long[]{1, 2},
        new long[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  private void updateSketch(final Object first, final Object... others)
  {
    // first != null check mimics how updateSketch is called: it's always guarded by a null check on the outer value.
    if (first != null) {
      HllSketchBuildAggregator.updateSketch(sketch, first);
    }

    for (final Object o : others) {
      if (o != null) {
        HllSketchBuildAggregator.updateSketch(sketch, o);
      }
    }
  }

  private void assertSketchEstimate(final long estimate)
  {
    Assert.assertEquals((double) estimate, sketch.getEstimate(), 0.1);
  }
}
