/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.collect.Lists;
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
        Assert.assertTrue(String.format("Clone[%d] out of bounds", j), aClone.withinBounds());
        Assert.assertEquals(String.format("Clone[%d] not right", j), expectedValues[i], aClone.getOffset());
        aClone.increment();
      }
      offsets.add(offsets.get(0).clone());
    }

    for (Offset aClone : offsets) {
      Assert.assertFalse(aClone.withinBounds());
    }
  }
}
