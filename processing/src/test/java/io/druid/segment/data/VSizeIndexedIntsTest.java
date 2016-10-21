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

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;

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

  @Test
  public void testGetBytesNoPaddingfromList() throws Exception
  {
    final int[] array = {1, 2, 4, 5, 6, 8, 9, 10};
    List<Integer> list = Ints.asList(array);
    int maxValue = Ints.max(array);
    VSizeIndexedInts ints = VSizeIndexedInts.fromList(list, maxValue);
    byte[] bytes1 = ints.getBytesNoPadding();
    byte[] bytes2 = VSizeIndexedInts.getBytesNoPaddingfromList(list, maxValue);
    Assert.assertArrayEquals(bytes1, bytes2);
  }
}
