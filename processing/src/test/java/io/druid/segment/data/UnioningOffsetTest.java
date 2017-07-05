/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.collect.Lists;
import io.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 */
public class UnioningOffsetTest
{
  @Test
  public void testSanity() throws Exception
  {
    assertExpected(
        new int[]{1, 2, 3, 4, 5, 6, 7, 8},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{2, 3, 4, 5, 6, 7})
        )
    );
    assertExpected(
        new int[]{1, 2, 3, 4, 5, 6, 7, 8},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{2, 3, 4, 5, 6, 7}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );

    assertExpected(
        new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{4, 5, 9, 10})
        )
    );

    assertExpected(
        new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{4, 5, 9, 10}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );

    assertExpected(
        new int[]{1, 2, 3, 6, 7, 8},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{})
        )
    );

    assertExpected(
        new int[]{1, 2, 3, 6, 7, 8},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );

    assertExpected(
        new int[]{1, 2, 3, 6, 7, 8},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{1}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );

    assertExpected(
        new int[]{1, 2, 3, 6, 7, 8},
        new UnioningOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{1})
        )
    );
  }

  private static void assertExpected(int[] expectedValues, UnioningOffset offset)
  {
    final ArrayList<Offset> offsets = Lists.newArrayList();
    offsets.add(offset);

    for (int i = 0; i < expectedValues.length; ++i) {
      for (int j = 0; j < offsets.size(); ++j) {
        Offset aClone = offsets.get(j);
        Assert.assertTrue(StringUtils.format("Clone[%d] out of bounds", j), aClone.withinBounds());
        Assert.assertEquals(StringUtils.format("Clone[%d] not right", j), expectedValues[i], aClone.getOffset());
        aClone.increment();
      }
      offsets.add(offsets.get(0).clone());
    }

    for (Offset aClone : offsets) {
      Assert.assertFalse(aClone.withinBounds());
    }
  }
}
