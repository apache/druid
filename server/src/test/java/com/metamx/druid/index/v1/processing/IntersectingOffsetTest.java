package com.metamx.druid.index.v1.processing;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;

/**
 */
public class IntersectingOffsetTest
{
  @Test
  public void testSanity() throws Exception
  {
    assertExpected(
        new int[]{2, 3, 6, 7},
        new IntersectingOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{2, 3, 4, 5, 6, 7})
        )
    );
    assertExpected(
        new int[]{2, 3, 6, 7},
        new IntersectingOffset(
            new ArrayBasedOffset(new int[]{2, 3, 4, 5, 6, 7}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );

    assertExpected(
        new int[]{},
        new IntersectingOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{4, 5, 9, 10})
        )
    );

    assertExpected(
        new int[]{},
        new IntersectingOffset(
            new ArrayBasedOffset(new int[]{4, 5, 9, 10}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );

    assertExpected(
        new int[]{},
        new IntersectingOffset(
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8}),
            new ArrayBasedOffset(new int[]{})
        )
    );

    assertExpected(
        new int[]{},
        new IntersectingOffset(
            new ArrayBasedOffset(new int[]{}),
            new ArrayBasedOffset(new int[]{1, 2, 3, 6, 7, 8})
        )
    );
  }


  private static void assertExpected(int[] expectedValues, IntersectingOffset offset)
  {
    final LinkedList<Offset> offsets = Lists.newLinkedList();
    offsets.add(offset);

    for (int i = 0; i < expectedValues.length; ++i) {
      for (Offset aClone : offsets) {
        Assert.assertTrue(aClone.withinBounds());
        Assert.assertEquals(expectedValues[i], aClone.getOffset());
        aClone.increment();
      }
      offsets.add(offsets.getFirst().clone());
    }

    for (Offset aClone : offsets) {
      Assert.assertFalse(aClone.withinBounds());
    }
  }
}
