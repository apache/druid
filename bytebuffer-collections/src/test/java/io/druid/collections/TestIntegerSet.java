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
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.WrappedBitSetBitmap;
import io.druid.collections.bitmap.WrappedConciseBitmap;
import io.druid.collections.bitmap.WrappedRoaringBitmap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 *
 */
public class TestIntegerSet
{
  private static Iterable<Class<? extends MutableBitmap>> clazzes = Lists.newArrayList(
      WrappedBitSetBitmap.class,
      WrappedConciseBitmap.class,
      WrappedRoaringBitmap.class
  );

  @Test
  public void testSimpleSet()
  {
    WrappedBitSetBitmap wrappedBitSetBitmapBitSet = new WrappedBitSetBitmap();
    IntSetTestUtility.addAllToMutable(wrappedBitSetBitmapBitSet, IntSetTestUtility.getSetBits());
    IntegerSet integerSet = IntegerSet.wrap(wrappedBitSetBitmapBitSet);

    Assert.assertTrue(Sets.difference(integerSet, IntSetTestUtility.getSetBits()).isEmpty());
  }

  @Test
  public void testSimpleAdd() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Set<Integer> set = IntSetTestUtility.getSetBits();
      set.add(999);
      integerSet.add(999);

      Assert.assertTrue(Sets.difference(integerSet, set).isEmpty());

      integerSet.add(58577);
      Assert.assertFalse(Sets.difference(integerSet, set).isEmpty());
    }
  }

  @Test
  public void testContainsAll() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Set<Integer> set = IntSetTestUtility.getSetBits();
      Assert.assertTrue(integerSet.containsAll(set));

      set.add(999);
      Assert.assertFalse(integerSet.containsAll(set));
    }
  }

  @Test
  public void testRemoveEverything() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Set<Integer> set = IntSetTestUtility.getSetBits();

      integerSet.removeAll(set);
      boolean isEmpty = integerSet.isEmpty();
      Assert.assertTrue(isEmpty);
    }
  }

  @Test
  public void testRemoveOneThing() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Set<Integer> set = IntSetTestUtility.getSetBits();

      integerSet.remove(1);
      set.remove(1);

      Assert.assertTrue(Sets.difference(set, integerSet).isEmpty());
    }
  }


  @Test
  public void testIsEmpty() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Assert.assertFalse(integerSet.isEmpty());

      integerSet.clear();

      Assert.assertTrue(integerSet.isEmpty());

      integerSet.add(1);
      Assert.assertFalse(integerSet.isEmpty());
    }
  }

  @Test
  public void testSize() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Set<Integer> set = IntSetTestUtility.getSetBits();

      Assert.assertEquals(set.size(), integerSet.size());
    }
  }


  @Test
  public void testRetainAll() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Set<Integer> set = IntSetTestUtility.getSetBits();

      set.remove(1);
      set.add(9999);

      boolean threwError = false;
      try {
        integerSet.retainAll(set);
      }
      catch (UnsupportedOperationException ex) {
        threwError = true;
      }
      Assert.assertTrue(threwError);
    }
  }

  @Test
  public void testIntOverflow() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      Exception e = null;
      try {
        MutableBitmap wrappedBitmap = clazz.newInstance();
        IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
        IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);
        integerSet.add(Integer.MAX_VALUE + 1);
      }
      catch (IllegalArgumentException ex) {
        e = ex;
      }
      Assert.assertNotNull(e);
    }
  }

  @Test
  public void testToArray() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      Exception e = null;
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);
      Set<Integer> set = Sets.newHashSet((Integer[]) integerSet.toArray());
      Assert.assertTrue(Sets.difference(integerSet, set).isEmpty());
    }
  }


  @Test
  public void testToSmallArray() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      Exception e = null;
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);
      Set<Integer> set = Sets.newHashSet((Integer[]) integerSet.toArray(new Integer[0]));
      Assert.assertTrue(Sets.difference(integerSet, set).isEmpty());
    }
  }


  @Test
  public void testToBigArray() throws IllegalAccessException, InstantiationException
  {
    for (Class<? extends MutableBitmap> clazz : clazzes) {
      Exception e = null;
      MutableBitmap wrappedBitmap = clazz.newInstance();
      IntSetTestUtility.addAllToMutable(wrappedBitmap, IntSetTestUtility.getSetBits());
      IntegerSet integerSet = IntegerSet.wrap(wrappedBitmap);

      Integer[] bigArray = new Integer[1024];
      integerSet.toArray(bigArray);
      Set<Integer> set = Sets.newHashSet(bigArray);
      Assert.assertTrue(Sets.difference(integerSet, set).isEmpty());
    }
  }
}
