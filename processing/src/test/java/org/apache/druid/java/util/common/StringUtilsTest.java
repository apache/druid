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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

  @Test
  public void fromUtf8ConversionTest() throws UnsupportedEncodingException
  {
    byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
    Assertions.assertEquals("abcd", StringUtils.fromUtf8(bytes));

    String abcd = "abcd";
    Assertions.assertEquals(abcd, StringUtils.fromUtf8(abcd.getBytes(StringUtils.UTF8_STRING)));
  }

  @Test
  public void toUtf8ConversionTest()
  {
    byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
    byte[] strBytes = StringUtils.toUtf8("abcd");
    for (int i = 0; i < bytes.length; ++i) {
      Assertions.assertEquals(bytes[i], strBytes[i]);
    }
  }

  @Test
  public void toUtf8WithLimitTest()
  {
    final ByteBuffer smallBuffer = ByteBuffer.allocate(4);
    final ByteBuffer mediumBuffer = ByteBuffer.allocate(6);
    final ByteBuffer bigBuffer = ByteBuffer.allocate(8);

    final int smallBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", smallBuffer);
    Assertions.assertEquals(4, smallBufferResult);
    final byte[] smallBufferByteArray = new byte[smallBufferResult];
    smallBuffer.get(smallBufferByteArray);
    Assertions.assertEquals("🚀", StringUtils.fromUtf8(smallBufferByteArray));

    final int mediumBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", mediumBuffer);
    Assertions.assertEquals(4, mediumBufferResult);
    final byte[] mediumBufferByteArray = new byte[mediumBufferResult];
    mediumBuffer.get(mediumBufferByteArray);
    Assertions.assertEquals("🚀", StringUtils.fromUtf8(mediumBufferByteArray));

    final int bigBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", bigBuffer);
    Assertions.assertEquals(8, bigBufferResult);
    final byte[] bigBufferByteArray = new byte[bigBufferResult];
    bigBuffer.get(bigBufferByteArray);
    Assertions.assertEquals("🚀🌔", StringUtils.fromUtf8(bigBufferByteArray));
  }

  @Test
  public void fromUtf8ByteBufferHeap()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    Assertions.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
    bytes.rewind();
    Assertions.assertEquals("abcd", StringUtils.fromUtf8(bytes));
  }

  @Test
  public void testMiddleOfByteArrayConversion()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    bytes.position(1).limit(3);
    Assertions.assertEquals("bc", StringUtils.fromUtf8(bytes, 2));
    bytes.position(1);
    Assertions.assertEquals("bc", StringUtils.fromUtf8(bytes));
  }


  @Test
  public void testOutOfBounds()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    bytes.position(1).limit(3);
    Assertions.assertThrows(BufferUnderflowException.class, () -> StringUtils.fromUtf8(bytes, 3));
  }

  @Test
  public void testNullPointerByteBuffer()
  {
    Assertions.assertThrows(NullPointerException.class, () -> StringUtils.fromUtf8((ByteBuffer) null));
  }

  @Test
  public void testNullPointerByteArray()
  {
    Assertions.assertThrows(NullPointerException.class, () -> StringUtils.fromUtf8((byte[]) null));
  }

  @Test
  public void fromUtf8ByteBufferDirect()
  {
    try (final ResourceHolder<ByteBuffer> bufferHolder = ByteBufferUtils.allocateDirect(4)) {
      final ByteBuffer bytes = bufferHolder.get();
      bytes.put(new byte[]{'a', 'b', 'c', 'd'});
      bytes.rewind();
      Assertions.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
      bytes.rewind();
      Assertions.assertEquals("abcd", StringUtils.fromUtf8(bytes));
    }
  }

  @SuppressWarnings("MalformedFormatString")
  @Test
  public void testNonStrictFormat()
  {
    Assertions.assertEquals("test%d; format", StringUtils.nonStrictFormat("test%d", "format"));
    Assertions.assertEquals(
        "test%s%s; format",
        // codeql[java/missing-format-argument]
        StringUtils.nonStrictFormat("test%s%s", "format")
    );
  }

  @Test
  public void testRemoveChar()
  {
    Assertions.assertEquals("123", StringUtils.removeChar("123", ','));
    Assertions.assertEquals("123", StringUtils.removeChar("123,", ','));
    Assertions.assertEquals("123", StringUtils.removeChar(",1,,2,3,", ','));
    Assertions.assertEquals("", StringUtils.removeChar(",,", ','));
  }

  @Test
  public void testReplaceChar()
  {
    Assertions.assertEquals("123", StringUtils.replaceChar("123", ',', "x"));
    Assertions.assertEquals("12345", StringUtils.replaceChar("123,", ',', "45"));
    Assertions.assertEquals("", StringUtils.replaceChar("", 'a', "bb"));
    Assertions.assertEquals("bb", StringUtils.replaceChar("a", 'a', "bb"));
    Assertions.assertEquals("bbbb", StringUtils.replaceChar("aa", 'a', "bb"));
  }

  @Test
  public void testReplace()
  {
    Assertions.assertEquals("x1x2x3x", StringUtils.replace("123", "", "x"));
    Assertions.assertEquals("12345", StringUtils.replace("123,", ",", "45"));
    Assertions.assertEquals("", StringUtils.replace("", "a", "bb"));
    Assertions.assertEquals("bb", StringUtils.replace("a", "a", "bb"));
    Assertions.assertEquals("bba", StringUtils.replace("aaa", "aa", "bb"));
    Assertions.assertEquals("bcb", StringUtils.replace("aacaa", "aa", "b"));
    Assertions.assertEquals("bb", StringUtils.replace("aaaa", "aa", "b"));
    Assertions.assertEquals("", StringUtils.replace("aaaa", "aa", ""));
  }

  @Test
  public void testEncodeForFormat()
  {
    Assertions.assertEquals("x %% a %%s", StringUtils.encodeForFormat("x % a %s"));
    Assertions.assertEquals("", StringUtils.encodeForFormat(""));
    Assertions.assertNull(StringUtils.encodeForFormat(null));
  }

  @Test
  public void testURLEncodeSpace()
  {
    String s1 = StringUtils.urlEncode("aaa bbb");
    Assertions.assertEquals(s1, "aaa%20bbb");
    Assertions.assertEquals("aaa bbb", StringUtils.urlDecode(s1));

    String s2 = StringUtils.urlEncode("fff+ggg");
    Assertions.assertEquals(s2, "fff%2Bggg");
    Assertions.assertEquals("fff+ggg", StringUtils.urlDecode(s2));
  }

  @Test
  public void testRepeat()
  {
    Assertions.assertEquals("", StringUtils.repeat("foo", 0));
    Assertions.assertEquals("foo", StringUtils.repeat("foo", 1));
    Assertions.assertEquals("foofoofoo", StringUtils.repeat("foo", 3));

    Assertions.assertEquals("", StringUtils.repeat("", 0));
    Assertions.assertEquals("", StringUtils.repeat("", 1));
    Assertions.assertEquals("", StringUtils.repeat("", 3));

    final IllegalArgumentException exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StringUtils.repeat("foo", -1)
    );
    Assertions.assertTrue(exception.getMessage().contains("count is negative, -1"));
  }

  @Test
  public void testLpad()
  {
    String lpad = StringUtils.lpad("abc", 7, "de");
    Assertions.assertEquals("dedeabc", lpad);

    lpad = StringUtils.lpad("abc", 6, "de");
    Assertions.assertEquals("dedabc", lpad);

    lpad = StringUtils.lpad("abc", 2, "de");
    Assertions.assertEquals("ab", lpad);

    lpad = StringUtils.lpad("abc", 0, "de");
    Assertions.assertEquals("", lpad);

    lpad = StringUtils.lpad("abc", -1, "de");
    Assertions.assertEquals("", lpad);

    lpad = StringUtils.lpad("abc", 10, "");
    Assertions.assertEquals("abc", lpad);

    lpad = StringUtils.lpad("abc", 1, "");
    Assertions.assertEquals("a", lpad);
  }

  @Test
  public void testRpad()
  {
    String rpad = StringUtils.rpad("abc", 7, "de");
    Assertions.assertEquals("abcdede", rpad);

    rpad = StringUtils.rpad("abc", 6, "de");
    Assertions.assertEquals("abcded", rpad);

    rpad = StringUtils.rpad("abc", 2, "de");
    Assertions.assertEquals("ab", rpad);

    rpad = StringUtils.rpad("abc", 0, "de");
    Assertions.assertEquals("", rpad);

    rpad = StringUtils.rpad("abc", -1, "de");
    Assertions.assertEquals("", rpad);

    rpad = StringUtils.rpad("abc", 10, "");
    Assertions.assertEquals("abc", rpad);

    rpad = StringUtils.rpad("abc", 1, "");
    Assertions.assertEquals("a", rpad);
  }

  @Test
  public void testChop()
  {
    Assertions.assertEquals("foo", StringUtils.chop("foo", 5));
    Assertions.assertEquals("fo", StringUtils.chop("foo", 2));
    Assertions.assertEquals("", StringUtils.chop("foo", 0));
    Assertions.assertEquals("smile 🙂 for", StringUtils.chop("smile 🙂 for the camera", 14));
    Assertions.assertEquals("smile 🙂", StringUtils.chop("smile 🙂 for the camera", 10));
    Assertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 9));
    Assertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 8));
    Assertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 7));
    Assertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 6));
    Assertions.assertEquals("smile", StringUtils.chop("smile 🙂 for the camera", 5));
  }

  @Test
  public void testFastLooseChop()
  {
    Assertions.assertEquals("foo", StringUtils.fastLooseChop("foo", 5));
    Assertions.assertEquals("fo", StringUtils.fastLooseChop("foo", 2));
    Assertions.assertEquals("", StringUtils.fastLooseChop("foo", 0));
    Assertions.assertEquals("smile 🙂 for", StringUtils.fastLooseChop("smile 🙂 for the camera", 12));
    Assertions.assertEquals("smile 🙂 ", StringUtils.fastLooseChop("smile 🙂 for the camera", 9));
    Assertions.assertEquals("smile 🙂", StringUtils.fastLooseChop("smile 🙂 for the camera", 8));
    Assertions.assertEquals("smile \uD83D", StringUtils.fastLooseChop("smile 🙂 for the camera", 7));
    Assertions.assertEquals("smile ", StringUtils.fastLooseChop("smile 🙂 for the camera", 6));
    Assertions.assertEquals("smile", StringUtils.fastLooseChop("smile 🙂 for the camera", 5));
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

        Assertions.assertEquals(
            (int) Math.signum(compareUtf8),
            (int) Math.signum(compareUnicode),
            StringUtils.format(
                "compareUnicode (actual) matches compareUtf8 (expected) for [%s] vs [%s]",
                string1,
                string2
            )
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

        Assertions.assertEquals(
            (int) Math.signum(compareJavaString),
            (int) Math.signum(compareByteArrayUtf8UsingJavaStringOrdering),
            StringUtils.format(
                "compareUtf8UsingJavaStringOrdering(byte[]) (actual) "
                + "matches compareJavaString (expected) for [%s] vs [%s]",
                string1,
                string2
            )
        );

        Assertions.assertEquals(
            (int) Math.signum(compareJavaString),
            (int) Math.signum(compareByteBufferUtf8UsingJavaStringOrdering),
            StringUtils.format(
                "compareByteBufferUtf8UsingJavaStringOrdering(ByteBuffer) (actual) "
                + "matches compareJavaString (expected) for [%s] vs [%s]",
                string1,
                string2
            )
        );
      }
    }
  }

  @Test()
  public void testNonStrictFormatWithNullMessage()
  {
    Assertions.assertThrows(NullPointerException.class, () -> StringUtils.nonStrictFormat(null, 1, 2));
  }

  @Test
  public void testNonStrictFormatWithStringContainingPercent()
  {
    Assertions.assertEquals(
        "some string containing % %s %d %f",
        StringUtils.nonStrictFormat("%s", "some string containing % %s %d %f")
    );
  }
}
