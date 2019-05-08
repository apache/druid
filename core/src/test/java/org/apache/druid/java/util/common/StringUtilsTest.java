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

package org.apache.druid.java.util.common;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 *
 */
public class StringUtilsTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

  @Test
  public void testRemoveChar()
  {
    Assert.assertEquals("123", StringUtils.removeChar("123", ','));
    Assert.assertEquals("123", StringUtils.removeChar("123,", ','));
    Assert.assertEquals("123", StringUtils.removeChar(",1,,2,3,", ','));
    Assert.assertEquals("", StringUtils.removeChar(",,", ','));
  }

  @Test
  public void testReplaceChar()
  {
    Assert.assertEquals("123", StringUtils.replaceChar("123", ',', "x"));
    Assert.assertEquals("12345", StringUtils.replaceChar("123,", ',', "45"));
    Assert.assertEquals("", StringUtils.replaceChar("", 'a', "bb"));
    Assert.assertEquals("bb", StringUtils.replaceChar("a", 'a', "bb"));
    Assert.assertEquals("bbbb", StringUtils.replaceChar("aa", 'a', "bb"));
  }

  @Test
  public void testReplace()
  {
    Assert.assertEquals("x1x2x3x", StringUtils.replace("123", "", "x"));
    Assert.assertEquals("12345", StringUtils.replace("123,", ",", "45"));
    Assert.assertEquals("", StringUtils.replace("", "a", "bb"));
    Assert.assertEquals("bb", StringUtils.replace("a", "a", "bb"));
    Assert.assertEquals("bba", StringUtils.replace("aaa", "aa", "bb"));
    Assert.assertEquals("bcb", StringUtils.replace("aacaa", "aa", "b"));
    Assert.assertEquals("bb", StringUtils.replace("aaaa", "aa", "b"));
    Assert.assertEquals("", StringUtils.replace("aaaa", "aa", ""));
  }

  @Test
  public void testURLEncodeSpace()
  {
    String s1 = StringUtils.urlEncode("aaa bbb");
    Assert.assertEquals(s1, "aaa%20bbb");
    Assert.assertEquals("aaa bbb", StringUtils.urlDecode(s1));

    String s2 = StringUtils.urlEncode("fff+ggg");
    Assert.assertEquals(s2, "fff%2Bggg");
    Assert.assertEquals("fff+ggg", StringUtils.urlDecode(s2));
  }

  @Test
  public void testRepeat()
  {
    Assert.assertEquals("", StringUtils.repeat("foo", 0));
    Assert.assertEquals("foo", StringUtils.repeat("foo", 1));
    Assert.assertEquals("foofoofoo", StringUtils.repeat("foo", 3));

    Assert.assertEquals("", StringUtils.repeat("", 0));
    Assert.assertEquals("", StringUtils.repeat("", 1));
    Assert.assertEquals("", StringUtils.repeat("", 3));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("count is negative, -1");
    Assert.assertEquals("", StringUtils.repeat("foo", -1));
  }
  
  @Test
  public void testLpad()
  {
    String s1 = StringUtils.lpad("abc", 7, "de");
    Assert.assertEquals(s1, "dedeabc");

    String s2 = StringUtils.lpad("abc", 6, "de");
    Assert.assertEquals(s2, "dedabc");

    String s3 = StringUtils.lpad("abc", 2, "de");
    Assert.assertEquals(s3, "ab");

    String s4 = StringUtils.lpad("abc", 0, "de");
    Assert.assertEquals(s4, "");

    String s5 = StringUtils.lpad("abc", -1, "de");
    Assert.assertEquals(s5, null);
  }

  @Test
  public void testRpad()
  {
    String s1 = StringUtils.rpad("abc", 7, "de");
    Assert.assertEquals(s1, "abcdede");

    String s2 = StringUtils.rpad("abc", 6, "de");
    Assert.assertEquals(s2, "abcded");

    String s3 = StringUtils.rpad("abc", 2, "de");
    Assert.assertEquals(s3, "ab");

    String s4 = StringUtils.rpad("abc", 0, "de");
    Assert.assertEquals(s4, "");

    String s5 = StringUtils.rpad("abc", -1, "de");
    Assert.assertEquals(s5, null);
  }

}
