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

package io.druid.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import org.roaringbitmap.IntIterator;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public class IntSetTestUtility
{

  private static Set<Integer> setBits = Sets.newTreeSet(Lists.newArrayList(1, 2, 3, 5, 8, 13, 21));

  public static Set<Integer> getSetBits()
  {
    return Sets.newTreeSet(setBits);
  }

  public static final BitSet createSimpleBitSet(Set<Integer> setBits)
  {
    BitSet retval = new BitSet();
    for (int i : setBits) {
      retval.set(i);
    }
    return retval;
  }

  public static final void addAllToMutable(MutableBitmap mutableBitmap, Iterable<Integer> intSet)
  {
    for (Integer integer : intSet) {
      mutableBitmap.add(integer);
    }
  }

  public static Boolean equalSets(Set<Integer> s1, ImmutableBitmap s2)
  {
    Set<Integer> s3 = new HashSet<>();
    for (Integer i : new IntIt(s2.iterator())) {
      s3.add(i);
    }
    return Sets.difference(s1, s3).isEmpty();
  }

  private static class IntIt implements Iterable<Integer>
  {
    private final Iterator<Integer> intIter;

    public IntIt(IntIterator intIt)
    {
      this.intIter = new IntIter(intIt);
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return intIter;
    }

    private static class IntIter implements Iterator<Integer>
    {
      private final IntIterator intIt;

      public IntIter(IntIterator intIt)
      {
        this.intIt = intIt;
      }

      @Override
      public boolean hasNext()
      {
        return intIt.hasNext();
      }

      @Override
      public Integer next()
      {
        return intIt.next();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("Cannot remove ints from int iterator");
      }
    }
  }
}
