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

package io.druid.java.util.common;

import junit.framework.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 *
 */
public class StringUtilsTest
{
  @Test
  public void fromUtf8ConversionTest() throws UnsupportedEncodingException
  {
    byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
    Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes));

    String abcd = "abcd";
    Assert.assertEquals(abcd, StringUtils.fromUtf8(abcd.getBytes(StringUtils.UTF8_STRING)));
  }

  @Test
  public void toUtf8ConversionTest()
  {
    byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
    byte[] strBytes = StringUtils.toUtf8("abcd");
    for (int i = 0; i < bytes.length; ++i) {
      Assert.assertEquals(bytes[i], strBytes[i]);
    }
  }

  @Test
  public void fromUtf8ByteBufferHeap()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
    bytes.rewind();
    Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes));
  }

  @Test
  public void testMiddleOfByteArrayConversion()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    bytes.position(1).limit(3);
    Assert.assertEquals("bc", StringUtils.fromUtf8(bytes, 2));
    bytes.position(1);
    Assert.assertEquals("bc", StringUtils.fromUtf8(bytes));
  }


  @Test(expected = BufferUnderflowException.class)
  public void testOutOfBounds()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    bytes.position(1).limit(3);
    StringUtils.fromUtf8(bytes, 3);
  }

  @Test(expected = NullPointerException.class)
  public void testNullPointerByteBuffer()
  {
    StringUtils.fromUtf8((ByteBuffer) null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullPointerByteArray()
  {
    StringUtils.fromUtf8((byte[]) null);
  }

  @Test
  public void fromUtf8ByteBufferDirect()
  {
    ByteBuffer bytes = ByteBuffer.allocateDirect(4);
    bytes.put(new byte[]{'a', 'b', 'c', 'd'});
    bytes.rewind();
    Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
    bytes.rewind();
    Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes));
  }

  @Test
  public void testCharsetShowsUpAsDeprecated()
  {
    // Not actually a runnable test, just checking the IDE
    Assert.assertNotNull(StringUtils.UTF8_CHARSET);
  }

  @SuppressWarnings("MalformedFormatString")
  @Test
  public void testNonStrictFormat()
  {
    Assert.assertEquals("test%d; format", StringUtils.nonStrictFormat("test%d", "format"));
    Assert.assertEquals("test%s%s; format", StringUtils.nonStrictFormat("test%s%s", "format"));
  }
}
