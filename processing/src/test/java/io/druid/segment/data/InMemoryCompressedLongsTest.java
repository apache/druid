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

package io.druid.segment.data;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteOrder;

/**
 */
public class InMemoryCompressedLongsTest
{
  private InMemoryCompressedLongs longs;
  private long[] vals;

  @Before
  public void setUp() throws Exception
  {
    longs = null;
    vals = null;
  }

  private void setupSimple()
  {
    vals = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    longs = new InMemoryCompressedLongs(
        5,
        ByteOrder.nativeOrder()
    );

    for (int i = 0; i < vals.length; i++) {
      Assert.assertEquals(i, longs.add(vals[i]));
    }
  }

  @Test
  public void testSanity() throws Exception
  {
    setupSimple();

    Assert.assertEquals(vals.length, longs.size());
    for (int i = 0; i < longs.size(); ++i) {
      Assert.assertEquals(vals[i], longs.get(i));
    }
  }

  @Test
  public void testBulkFill() throws Exception
  {
    setupSimple();

    tryFill(0, 16);
    tryFill(3, 6);
    tryFill(7, 7);
    tryFill(7, 9);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBulkFillTooMuch() throws Exception
  {
    setupSimple();
    tryFill(7, 10);
  }

  private void tryFill(final int startIndex, final int size)
  {
    long[] filled = new long[size];
    longs.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i]);
    }
  }

  @Test
  public void testCanConvertToCompressedLongsIndexedSupplier() throws Exception
  {
    setupSimple();

    IndexedLongs indexed = longs.toCompressedLongsIndexedSupplier().get();

    for (int i = 0; i < longs.size(); i++) {
      Assert.assertEquals(longs.get(i), indexed.get(i));
    }

    indexed.close();
  }
}
