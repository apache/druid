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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class WrappedRoaringBitmapTest
{
  private static final int[] DATA = new int[]{1, 3, 5, 7, 9, 10, 11, 100, 122};

  private final int cardinality;
  private WrappedRoaringBitmap bitmap;

  public WrappedRoaringBitmapTest(int cardinality)
  {
    this.cardinality = cardinality;
  }

  @Parameterized.Parameters
  public static List<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (int i = 0; i < DATA.length; i++) {
      constructors.add(new Object[]{i});
    }
    return constructors;
  }

  @Before
  public void setUp()
  {
    bitmap = (WrappedRoaringBitmap) RoaringBitmapFactory.INSTANCE.makeEmptyMutableBitmap();
    for (int i = 0; i < cardinality; i++) {
      bitmap.add(DATA[i]);
    }
  }

  @Test
  public void testGet()
  {
    for (int i = 0; i < DATA.length; i++) {
      Assert.assertEquals(String.valueOf(i), i < cardinality, bitmap.get(DATA[i]));
    }

    Assert.assertFalse(bitmap.get(-1));
    Assert.assertFalse(bitmap.get(Integer.MAX_VALUE));
  }

  @Test
  public void testSize()
  {
    Assert.assertEquals(cardinality, bitmap.size());
  }

  @Test
  public void testRemove()
  {
    bitmap.remove(Integer.MAX_VALUE);
    Assert.assertEquals(cardinality, bitmap.size());

    if (cardinality > 0) {
      bitmap.remove(DATA[0]);
      Assert.assertEquals(cardinality - 1, bitmap.size());
    }
  }

  @Test
  public void testClear()
  {
    bitmap.clear();
    Assert.assertEquals(0, bitmap.size());
  }

  @Test
  public void testIterator()
  {
    final IntIterator iterator = bitmap.iterator();

    int i = 0;
    while (iterator.hasNext()) {
      final int n = iterator.next();
      Assert.assertEquals(String.valueOf(i), DATA[i], n);
      i++;
    }
    Assert.assertEquals("number of elements", i, cardinality);
  }

  @Test
  public void testSerialize()
  {
    byte[] buffer = new byte[bitmap.getSizeInBytes()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    bitmap.serialize(byteBuffer);
    byteBuffer.flip();
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().mapImmutableBitmap(byteBuffer);
    Assert.assertEquals(cardinality, immutableBitmap.size());
  }

  @Test
  public void testToByteArray()
  {
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().mapImmutableBitmap(ByteBuffer.wrap(bitmap.toBytes()));
    Assert.assertEquals(cardinality, immutableBitmap.size());
  }
}
