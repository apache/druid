package com.metamx.druid.index.v1;

import com.metamx.druid.index.v1.processing.Offset;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ConciseOffsetTest
{
  @Test
  public void testSanity() throws Exception
  {
    int[] vals = {1, 2, 4, 291, 27412, 49120, 212312, 2412101};
    ConciseSet mutableSet = new ConciseSet();
    for (int val : vals) {
      mutableSet.add(val);
    }

    ImmutableConciseSet set = ImmutableConciseSet.newImmutableFromMutable(mutableSet);

    ConciseOffset offset = new ConciseOffset(set);

    int count = 0;
    while (offset.withinBounds()) {
      Assert.assertEquals(vals[count], offset.getOffset());

      int cloneCount = count;
      Offset clonedOffset = offset.clone();
      while (clonedOffset.withinBounds()) {
        Assert.assertEquals(vals[cloneCount], clonedOffset.getOffset());

        ++cloneCount;
        clonedOffset.increment();
      }

      ++count;
      offset.increment();
    }
    Assert.assertEquals(count, vals.length);
  }
}
