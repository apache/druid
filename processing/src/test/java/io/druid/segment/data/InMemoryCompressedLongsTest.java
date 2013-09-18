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
