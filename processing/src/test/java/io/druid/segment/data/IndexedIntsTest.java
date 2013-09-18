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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class IndexedIntsTest
{
  private static final int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  private final IndexedInts indexed;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[][]{
            {VSizeIndexedInts.fromArray(array)},
            {IntBufferIndexedInts.fromArray(array)}
        }
    );
  }

  public IndexedIntsTest(
      IndexedInts indexed
  )
  {
    this.indexed = indexed;
  }

  @Test
  public void testSanity() throws Exception
  {
    Assert.assertEquals(array.length, indexed.size());
    for (int i = 0; i < array.length; i++) {
      Assert.assertEquals(array[i], indexed.get(i));
    }
  }
}
