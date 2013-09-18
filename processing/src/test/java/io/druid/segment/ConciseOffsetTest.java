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

package io.druid.segment;

import io.druid.segment.data.Offset;
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
