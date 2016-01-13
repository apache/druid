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

package io.druid.segment;

import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.WrappedImmutableConciseBitmap;
import io.druid.segment.data.Offset;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class BitmapOffsetTest
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

    BitmapOffset offset = new BitmapOffset(new ConciseBitmapFactory(), new WrappedImmutableConciseBitmap(set), false);

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
