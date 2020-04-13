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

package org.apache.druid.collections.bitmap;

import com.google.common.collect.Sets;
import org.apache.druid.collections.IntSetTestUtility;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class WrappedBitSetBitmapBitSetTest
{

  private static WrappedBitSetBitmap defaultBitSet()
  {
    return new WrappedBitSetBitmap(IntSetTestUtility.createSimpleBitSet(IntSetTestUtility.getSetBits()));
  }

  @Test
  public void testIterator()
  {
    WrappedBitSetBitmap bitSet = new WrappedBitSetBitmap();
    for (int i : IntSetTestUtility.getSetBits()) {
      bitSet.add(i);
    }
    IntIterator intIt = bitSet.iterator();
    for (int i : IntSetTestUtility.getSetBits()) {
      Assert.assertTrue(intIt.hasNext());
      Assert.assertEquals(i, intIt.next());
    }
  }

  @Test
  public void testSize()
  {
    BitSet bitSet = IntSetTestUtility.createSimpleBitSet(IntSetTestUtility.getSetBits());
    WrappedBitSetBitmap wrappedBitSetBitmapBitSet = new WrappedBitSetBitmap(bitSet);
    Assert.assertEquals(bitSet.cardinality(), wrappedBitSetBitmapBitSet.size());
  }

  @Test
  public void testOffHeap()
  {
    ByteBuffer buffer = ByteBuffer.allocateDirect(Long.SIZE * 100 / 8).order(ByteOrder.LITTLE_ENDIAN);
    BitSet testSet = BitSet.valueOf(buffer);
    testSet.set(1);
    WrappedImmutableBitSetBitmap bitMap = new WrappedImmutableBitSetBitmap(testSet);
    Assert.assertTrue(bitMap.get(1));
    testSet.set(2);
    Assert.assertTrue(bitMap.get(2));
  }

  @Test
  public void testSimpleBitSet()
  {
    WrappedBitSetBitmap bitSet = new WrappedBitSetBitmap(IntSetTestUtility.createSimpleBitSet(IntSetTestUtility.getSetBits()));
    Assert.assertTrue(IntSetTestUtility.equalSets(IntSetTestUtility.getSetBits(), bitSet));
  }

  @Test
  public void testUnion()
  {
    WrappedBitSetBitmap bitSet = new WrappedBitSetBitmap(IntSetTestUtility.createSimpleBitSet(IntSetTestUtility.getSetBits()));

    Set<Integer> extraBits = Sets.newHashSet(6, 9);
    WrappedBitSetBitmap bitExtraSet = new WrappedBitSetBitmap(IntSetTestUtility.createSimpleBitSet(extraBits));

    Set<Integer> union = Sets.union(extraBits, IntSetTestUtility.getSetBits());

    Assert.assertTrue(IntSetTestUtility.equalSets(union, (WrappedBitSetBitmap) bitSet.union(bitExtraSet)));
  }

  @Test
  public void testIntersection()
  {
    WrappedBitSetBitmap bitSet = new WrappedBitSetBitmap(IntSetTestUtility.createSimpleBitSet(IntSetTestUtility.getSetBits()));

    Set<Integer> extraBits = Sets.newHashSet(1, 2, 3, 4, 5, 6, 7, 8);
    WrappedBitSetBitmap bitExtraSet = new WrappedBitSetBitmap(IntSetTestUtility.createSimpleBitSet(extraBits));

    Set<Integer> intersection = Sets.intersection(extraBits, IntSetTestUtility.getSetBits());

    Assert.assertTrue(IntSetTestUtility.equalSets(
        intersection,
        (WrappedBitSetBitmap) bitSet.intersection(bitExtraSet)
    ));
  }

  @Test
  public void testAnd()
  {
    WrappedBitSetBitmap bitSet = defaultBitSet();
    WrappedBitSetBitmap bitSet2 = defaultBitSet();
    Set<Integer> defaultBitSet = IntSetTestUtility.getSetBits();
    bitSet.remove(1);
    bitSet2.remove(2);

    bitSet.and(bitSet2);

    defaultBitSet.remove(1);
    defaultBitSet.remove(2);

    Assert.assertTrue(IntSetTestUtility.equalSets(defaultBitSet, bitSet));
  }


  @Test
  public void testOr()
  {
    WrappedBitSetBitmap bitSet = defaultBitSet();
    WrappedBitSetBitmap bitSet2 = defaultBitSet();
    Set<Integer> defaultBitSet = IntSetTestUtility.getSetBits();
    bitSet.remove(1);
    bitSet2.remove(2);

    bitSet.or(bitSet2);

    Assert.assertTrue(IntSetTestUtility.equalSets(defaultBitSet, bitSet));
  }

  @Test
  public void testAndNot()
  {
    WrappedBitSetBitmap bitSet = defaultBitSet();
    WrappedBitSetBitmap bitSet2 = defaultBitSet();
    Set<Integer> defaultBitSet = new HashSet<>();
    bitSet.remove(1);
    bitSet2.remove(2);

    bitSet.andNot(bitSet2);

    defaultBitSet.add(2);

    Assert.assertTrue(IntSetTestUtility.equalSets(defaultBitSet, bitSet));
  }


  @Test
  public void testSerialize()
  {
    WrappedBitSetBitmap bitSet = defaultBitSet();
    byte[] buffer = new byte[bitSet.getSizeInBytes()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    bitSet.serialize(byteBuffer);
  }
}
