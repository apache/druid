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

import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.ResourceHolder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.UnsupportedEncodingException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class StringUtilsTest
{
  private static final List<String> COMPARE_TEST_STRINGS = ImmutableList.of(
      "（請參見已被刪除版本）",
      "請參見已被刪除版本",
      "שָׁלוֹם",
      "＋{{[[Template:別名重定向|別名重定向]]}}",
      "\uD83D\uDC4D\uD83D\uDC4D\uD83D\uDC4D",
      "\uD83D\uDCA9",
      "",
      "f",
      "fo",
      "\uD83D\uDE42",
      "\uD83E\uDEE5",
      "\uD83E\uDD20",
      "quick",
      "brown",
      "fox"
  );

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
  public void toUtf8WithLimitTest()
  {
    final ByteBuffer smallBuffer = ByteBuffer.allocate(4);
    final ByteBuffer mediumBuffer = ByteBuffer.allocate(6);
    final ByteBuffer bigBuffer = ByteBuffer.allocate(8);

    final int smallBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", smallBuffer);
    Assert.assertEquals(4, smallBufferResult);
    final byte[] smallBufferByteArray = new byte[smallBufferResult];
    smallBuffer.get(smallBufferByteArray);
    Assert.assertEquals("🚀", StringUtils.fromUtf8(smallBufferByteArray));

    final int mediumBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", mediumBuffer);
    Assert.assertEquals(4, mediumBufferResult);
    final byte[] mediumBufferByteArray = new byte[mediumBufferResult];
    mediumBuffer.get(mediumBufferByteArray);
    Assert.assertEquals("🚀", StringUtils.fromUtf8(mediumBufferByteArray));

    final int bigBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", bigBuffer);
    Assert.assertEquals(8, bigBufferResult);
    final byte[] bigBufferByteArray = new byte[bigBufferResult];
    bigBuffer.get(bigBufferByteArray);
    Assert.assertEquals("🚀🌔", StringUtils.fromUtf8(bigBufferByteArray));
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
    try (final ResourceHolder<ByteBuffer> bufferHolder = ByteBufferUtils.allocateDirect(4)) {
      final ByteBuffer bytes = bufferHolder.get();
      bytes.put(new byte[]{'a', 'b', 'c', 'd'});
      bytes.rewind();
      Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
      bytes.rewind();
      Assert.assertEquals("abcd", StringUtils.fromUtf8(bytes));
    }
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
  public void testEncodeForFormat()
  {
    Assert.assertEquals("x %% a %%s", StringUtils.encodeForFormat("x % a %s"));
    Assert.assertEquals("", StringUtils.encodeForFormat(""));
    Assert.assertNull(StringUtils.encodeForFormat(null));
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
    String lpad = StringUtils.lpad("abc", 7, "de");
    Assert.assertEquals("dedeabc", lpad);

    lpad = StringUtils.lpad("abc", 6, "de");
    Assert.assertEquals("dedabc", lpad);

    lpad = StringUtils.lpad("abc", 2, "de");
    Assert.assertEquals("ab", lpad);

    lpad = StringUtils.lpad("abc", 0, "de");
    Assert.assertEquals("", lpad);

    lpad = StringUtils.lpad("abc", -1, "de");
    Assert.assertEquals("", lpad);

    lpad = StringUtils.lpad("abc", 10, "");
    Assert.assertEquals("abc", lpad);

    lpad = StringUtils.lpad("abc", 1, "");
    Assert.assertEquals("a", lpad);
  }

  @Test
  public void testRpad()
  {
    String rpad = StringUtils.rpad("abc", 7, "de");
    Assert.assertEquals("abcdede", rpad);

    rpad = StringUtils.rpad("abc", 6, "de");
    Assert.assertEquals("abcded", rpad);

    rpad = StringUtils.rpad("abc", 2, "de");
    Assert.assertEquals("ab", rpad);

    rpad = StringUtils.rpad("abc", 0, "de");
    Assert.assertEquals("", rpad);

    rpad = StringUtils.rpad("abc", -1, "de");
    Assert.assertEquals("", rpad);

    rpad = StringUtils.rpad("abc", 10, "");
    Assert.assertEquals("abc", rpad);

    rpad = StringUtils.rpad("abc", 1, "");
    Assert.assertEquals("a", rpad);
  }

  @Test
  public void testChop()
  {
    Assert.assertEquals("foo", StringUtils.chop("foo", 5));
    Assert.assertEquals("fo", StringUtils.chop("foo", 2));
    Assert.assertEquals("", StringUtils.chop("foo", 0));
    Assert.assertEquals("smile 🙂 for", StringUtils.chop("smile 🙂 for the camera", 14));
    Assert.assertEquals("smile 🙂", StringUtils.chop("smile 🙂 for the camera", 10));
    Assert.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 9));
    Assert.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 8));
    Assert.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 7));
    Assert.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 6));
    Assert.assertEquals("smile", StringUtils.chop("smile 🙂 for the camera", 5));
  }

  @Test
  public void testFastLooseChop()
  {
    Assert.assertEquals("foo", StringUtils.fastLooseChop("foo", 5));
    Assert.assertEquals("fo", StringUtils.fastLooseChop("foo", 2));
    Assert.assertEquals("", StringUtils.fastLooseChop("foo", 0));
    Assert.assertEquals("smile 🙂 for", StringUtils.fastLooseChop("smile 🙂 for the camera", 12));
    Assert.assertEquals("smile 🙂 ", StringUtils.fastLooseChop("smile 🙂 for the camera", 9));
    Assert.assertEquals("smile 🙂", StringUtils.fastLooseChop("smile 🙂 for the camera", 8));
    Assert.assertEquals("smile \uD83D", StringUtils.fastLooseChop("smile 🙂 for the camera", 7));
    Assert.assertEquals("smile ", StringUtils.fastLooseChop("smile 🙂 for the camera", 6));
    Assert.assertEquals("smile", StringUtils.fastLooseChop("smile 🙂 for the camera", 5));
  }

  @Test
  public void testUnicodeStringCompare()
  {
    for (final String string1 : COMPARE_TEST_STRINGS) {
      for (final String string2 : COMPARE_TEST_STRINGS) {
        final int compareUnicode = StringUtils.compareUnicode(string1, string2);
        final int compareUtf8 = StringUtils.compareUtf8(
            StringUtils.toUtf8(string1),
            StringUtils.toUtf8(string2)
        );

        Assert.assertEquals(
            StringUtils.format(
                "compareUnicode (actual) matches compareUtf8 (expected) for [%s] vs [%s]",
                string1,
                string2
            ),
            (int) Math.signum(compareUtf8),
            (int) Math.signum(compareUnicode)
        );
      }
    }
  }

  @Test
  public void testJavaStringCompare()
  {
    for (final String string1 : COMPARE_TEST_STRINGS) {
      for (final String string2 : COMPARE_TEST_STRINGS) {
        final int compareJavaString = string1.compareTo(string2);

        final byte[] utf8Bytes1 = StringUtils.toUtf8(string1);
        final byte[] utf8Bytes2 = StringUtils.toUtf8(string2);
        final int compareByteArrayUtf8UsingJavaStringOrdering =
            StringUtils.compareUtf8UsingJavaStringOrdering(utf8Bytes1, utf8Bytes2);

        final ByteBuffer utf8ByteBuffer1 = ByteBuffer.allocate(utf8Bytes1.length + 2);
        final ByteBuffer utf8ByteBuffer2 = ByteBuffer.allocate(utf8Bytes2.length + 2);
        utf8ByteBuffer1.position(1);
        utf8ByteBuffer1.put(utf8Bytes1, 0, utf8Bytes1.length).position(utf8Bytes1.length);
        utf8ByteBuffer2.position(1);
        utf8ByteBuffer2.put(utf8Bytes2, 0, utf8Bytes2.length).position(utf8Bytes2.length);
        final int compareByteBufferUtf8UsingJavaStringOrdering = StringUtils.compareUtf8UsingJavaStringOrdering(
            utf8ByteBuffer1,
            1,
            utf8Bytes1.length,
            utf8ByteBuffer2,
            1,
            utf8Bytes2.length
        );

        Assert.assertEquals(
            StringUtils.format(
                "compareUtf8UsingJavaStringOrdering(byte[]) (actual) "
                + "matches compareJavaString (expected) for [%s] vs [%s]",
                string1,
                string2
            ),
            (int) Math.signum(compareJavaString),
            (int) Math.signum(compareByteArrayUtf8UsingJavaStringOrdering)
        );

        Assert.assertEquals(
            StringUtils.format(
                "compareByteBufferUtf8UsingJavaStringOrdering(ByteBuffer) (actual) "
                + "matches compareJavaString (expected) for [%s] vs [%s]",
                string1,
                string2
            ),
            (int) Math.signum(compareJavaString),
            (int) Math.signum(compareByteBufferUtf8UsingJavaStringOrdering)
        );
      }
    }
  }
}
