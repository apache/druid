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

package io.druid.collections.bitmap;

import java.util.Arrays;
import java.util.Set;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import junit.framework.Assert;

public class ConciseBitmapFactoryTest
{
  @Test
  public void testUnwrapWithNull() throws Exception
  {
    ConciseBitmapFactory factory = new ConciseBitmapFactory();

    ImmutableBitmap bitmap = factory.union(
        Iterables.transform(
            Lists.newArrayList(new WrappedConciseBitmap()),
            new Function<WrappedConciseBitmap, ImmutableBitmap>()
            {
              @Override
              public ImmutableBitmap apply(WrappedConciseBitmap input)
              {
                return null;
              }
            }
        )
    );

    Assert.assertEquals(0, bitmap.size());
  }

  @Test
  public void testUnwrapMerge() throws Exception
  {
    ConciseBitmapFactory factory = new ConciseBitmapFactory();

    WrappedConciseBitmap set = new WrappedConciseBitmap();
    set.add(1);
    set.add(3);
    set.add(5);

    ImmutableBitmap bitmap = factory.union(
        Arrays.asList(
            factory.makeImmutableBitmap(set),
            null
        )
    );

    Assert.assertEquals(3, bitmap.size());
  }

  @Test
  public void testGetOutOfBounds()
  {
    final ConciseSet conciseSet = new ConciseSet();
    final Set<Integer> ints = ImmutableSet.of(0, 4, 9);
    for (int i : ints) {
      conciseSet.add(i);
    }
    final ImmutableBitmap immutableBitmap = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(conciseSet));
    final MutableBitmap mutableBitmap = new WrappedConciseBitmap(conciseSet);
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(Integer.toString(i), ints.contains(i), mutableBitmap.get(i));
      Assert.assertEquals(Integer.toString(i), ints.contains(i), immutableBitmap.get(i));
    }
  }
}
