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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;

/**
 */
public class VSizeIndexedTest
{
  @Test
  public void testSanity() throws Exception
  {
    List<int[]> someInts = Arrays.asList(
        new int[]{1, 2, 3, 4, 5},
        new int[]{6, 7, 8, 9, 10},
        new int[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
    );

    VSizeIndexed indexed = VSizeIndexed.fromIterable(
        Iterables.transform(
            someInts,
            new Function<int[], VSizeIndexedInts>()
            {
              @Override
              public VSizeIndexedInts apply(int[] input)
              {
                return VSizeIndexedInts.fromArray(input, 20);
              }
            }
        )
    );

    assertSame(someInts, indexed);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    indexed.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(indexed.getSerializedSize(), bytes.length);
    VSizeIndexed deserializedIndexed = VSizeIndexed.readFromByteBuffer(ByteBuffer.wrap(bytes));

    assertSame(someInts, deserializedIndexed);
  }

  private void assertSame(List<int[]> someInts, VSizeIndexed indexed)
  {
    Assert.assertEquals(3, indexed.size());
    for (int i = 0; i < indexed.size(); ++i) {
      final int[] ints = someInts.get(i);
      final VSizeIndexedInts vSizeIndexedInts = indexed.get(i);

      Assert.assertEquals(ints.length, vSizeIndexedInts.size());
      Assert.assertEquals(1, vSizeIndexedInts.getNumBytes());
      for (int j = 0; j < ints.length; j++) {
          Assert.assertEquals(ints[j], vSizeIndexedInts.get(j));
      }
    }
  }
}
