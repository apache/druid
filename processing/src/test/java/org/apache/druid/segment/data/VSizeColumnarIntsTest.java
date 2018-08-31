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

package org.apache.druid.segment.data;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

/**
 */
public class VSizeColumnarIntsTest
{
  @Test
  public void testSanity()
  {
    final int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    VSizeColumnarInts ints = VSizeColumnarInts.fromArray(array);

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
    VSizeColumnarInts ints = VSizeColumnarInts.fromArray(array);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ints.writeTo(Channels.newChannel(baos), null);

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(ints.getSerializedSize(), bytes.length);
    VSizeColumnarInts deserialized = VSizeColumnarInts.readFromByteBuffer(ByteBuffer.wrap(bytes));

    Assert.assertEquals(1, deserialized.getNumBytes());
    Assert.assertEquals(array.length, deserialized.size());
    for (int i = 0; i < array.length; i++) {
      Assert.assertEquals(array[i], deserialized.get(i));
    }
  }
}
