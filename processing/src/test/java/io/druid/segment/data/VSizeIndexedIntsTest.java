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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

/**
 */
public class VSizeIndexedIntsTest
{
  @Test
  public void testSanity() throws Exception
  {
    final int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    VSizeIndexedInts ints = VSizeIndexedInts.fromArray(array);

    Assert.assertEquals(1, ints.getNumBytes());
    Assert.assertEquals(array.length, ints.size());
    for (int i = 0; i < array.length; i++) {
      Assert.assertEquals(array[i], ints.get(i));
    }
  }

  @Test
  public void testSerialization() throws Exception
  {
    final int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    VSizeIndexedInts ints = VSizeIndexedInts.fromArray(array);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ints.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(ints.getSerializedSize(), bytes.length);
    VSizeIndexedInts deserialized = VSizeIndexedInts.readFromByteBuffer(ByteBuffer.wrap(bytes));

    Assert.assertEquals(1, deserialized.getNumBytes());
    Assert.assertEquals(array.length, deserialized.size());
    for (int i = 0; i < array.length; i++) {
      Assert.assertEquals(array[i], deserialized.get(i));
    }
  }

}
