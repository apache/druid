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
import org.apache.druid.testing.JupiterAssertions;
import org.apache.druid.testing.ThrowableExpectation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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

  @RegisterExtension
  public ThrowableExpectation expectedException = ThrowableExpectation.none();

  @Test
  public void fromUtf8ConversionTest() throws UnsupportedEncodingException
  {
    byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
    JupiterAssertions.assertEquals("abcd", StringUtils.fromUtf8(bytes));

    String abcd = "abcd";
    JupiterAssertions.assertEquals(abcd, StringUtils.fromUtf8(abcd.getBytes(StringUtils.UTF8_STRING)));
  }

  @Test
  public void toUtf8ConversionTest()
  {
    byte[] bytes = new byte[]{'a', 'b', 'c', 'd'};
    byte[] strBytes = StringUtils.toUtf8("abcd");
    for (int i = 0; i < bytes.length; ++i) {
      JupiterAssertions.assertEquals(bytes[i], strBytes[i]);
    }
  }

  @Test
  public void toUtf8WithLimitTest()
  {
    final ByteBuffer smallBuffer = ByteBuffer.allocate(4);
    final ByteBuffer mediumBuffer = ByteBuffer.allocate(6);
    final ByteBuffer bigBuffer = ByteBuffer.allocate(8);

    final int smallBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", smallBuffer);
    JupiterAssertions.assertEquals(4, smallBufferResult);
    final byte[] smallBufferByteArray = new byte[smallBufferResult];
    smallBuffer.get(smallBufferByteArray);
    JupiterAssertions.assertEquals("🚀", StringUtils.fromUtf8(smallBufferByteArray));

    final int mediumBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", mediumBuffer);
    JupiterAssertions.assertEquals(4, mediumBufferResult);
    final byte[] mediumBufferByteArray = new byte[mediumBufferResult];
    mediumBuffer.get(mediumBufferByteArray);
    JupiterAssertions.assertEquals("🚀", StringUtils.fromUtf8(mediumBufferByteArray));

    final int bigBufferResult = StringUtils.toUtf8WithLimit("🚀🌔", bigBuffer);
    JupiterAssertions.assertEquals(8, bigBufferResult);
    final byte[] bigBufferByteArray = new byte[bigBufferResult];
    bigBuffer.get(bigBufferByteArray);
    JupiterAssertions.assertEquals("🚀🌔", StringUtils.fromUtf8(bigBufferByteArray));
  }

  @Test
  public void fromUtf8ByteBufferHeap()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    JupiterAssertions.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
    bytes.rewind();
    JupiterAssertions.assertEquals("abcd", StringUtils.fromUtf8(bytes));
  }

  @Test
  public void testMiddleOfByteArrayConversion()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    bytes.position(1).limit(3);
    JupiterAssertions.assertEquals("bc", StringUtils.fromUtf8(bytes, 2));
    bytes.position(1);
    JupiterAssertions.assertEquals("bc", StringUtils.fromUtf8(bytes));
  }


  @Test
  @org.apache.druid.testing.ExpectThrows(BufferUnderflowException.class)
  public void testOutOfBounds()
  {
    ByteBuffer bytes = ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'});
    bytes.position(1).limit(3);
    StringUtils.fromUtf8(bytes, 3);
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(NullPointerException.class)
  public void testNullPointerByteBuffer()
  {
    StringUtils.fromUtf8((ByteBuffer) null);
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(NullPointerException.class)
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
      JupiterAssertions.assertEquals("abcd", StringUtils.fromUtf8(bytes, 4));
      bytes.rewind();
      JupiterAssertions.assertEquals("abcd", StringUtils.fromUtf8(bytes));
    }
  }

  @SuppressWarnings("MalformedFormatString")
  @Test
  public void testNonStrictFormat()
  {
    JupiterAssertions.assertEquals("test%d; format", StringUtils.nonStrictFormat("test%d", "format"));
    JupiterAssertions.assertEquals("test%s%s; format", StringUtils.nonStrictFormat("test%s%s", "format"));
  }

  @Test
  public void testRemoveChar()
  {
    JupiterAssertions.assertEquals("123", StringUtils.removeChar("123", ','));
    JupiterAssertions.assertEquals("123", StringUtils.removeChar("123,", ','));
    JupiterAssertions.assertEquals("123", StringUtils.removeChar(",1,,2,3,", ','));
    JupiterAssertions.assertEquals("", StringUtils.removeChar(",,", ','));
  }

  @Test
  public void testReplaceChar()
  {
    JupiterAssertions.assertEquals("123", StringUtils.replaceChar("123", ',', "x"));
    JupiterAssertions.assertEquals("12345", StringUtils.replaceChar("123,", ',', "45"));
    JupiterAssertions.assertEquals("", StringUtils.replaceChar("", 'a', "bb"));
    JupiterAssertions.assertEquals("bb", StringUtils.replaceChar("a", 'a', "bb"));
    JupiterAssertions.assertEquals("bbbb", StringUtils.replaceChar("aa", 'a', "bb"));
  }

  @Test
  public void testReplace()
  {
    JupiterAssertions.assertEquals("x1x2x3x", StringUtils.replace("123", "", "x"));
    JupiterAssertions.assertEquals("12345", StringUtils.replace("123,", ",", "45"));
    JupiterAssertions.assertEquals("", StringUtils.replace("", "a", "bb"));
    JupiterAssertions.assertEquals("bb", StringUtils.replace("a", "a", "bb"));
    JupiterAssertions.assertEquals("bba", StringUtils.replace("aaa", "aa", "bb"));
    JupiterAssertions.assertEquals("bcb", StringUtils.replace("aacaa", "aa", "b"));
    JupiterAssertions.assertEquals("bb", StringUtils.replace("aaaa", "aa", "b"));
    JupiterAssertions.assertEquals("", StringUtils.replace("aaaa", "aa", ""));
  }

  @Test
  public void testEncodeForFormat()
  {
    JupiterAssertions.assertEquals("x %% a %%s", StringUtils.encodeForFormat("x % a %s"));
    JupiterAssertions.assertEquals("", StringUtils.encodeForFormat(""));
    JupiterAssertions.assertNull(StringUtils.encodeForFormat(null));
  }

  @Test
  public void testURLEncodeSpace()
  {
    String s1 = StringUtils.urlEncode("aaa bbb");
    JupiterAssertions.assertEquals(s1, "aaa%20bbb");
    JupiterAssertions.assertEquals("aaa bbb", StringUtils.urlDecode(s1));

    String s2 = StringUtils.urlEncode("fff+ggg");
    JupiterAssertions.assertEquals(s2, "fff%2Bggg");
    JupiterAssertions.assertEquals("fff+ggg", StringUtils.urlDecode(s2));
  }

  @Test
  public void testRepeat()
  {
    JupiterAssertions.assertEquals("", StringUtils.repeat("foo", 0));
    JupiterAssertions.assertEquals("foo", StringUtils.repeat("foo", 1));
    JupiterAssertions.assertEquals("foofoofoo", StringUtils.repeat("foo", 3));

    JupiterAssertions.assertEquals("", StringUtils.repeat("", 0));
    JupiterAssertions.assertEquals("", StringUtils.repeat("", 1));
    JupiterAssertions.assertEquals("", StringUtils.repeat("", 3));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("count is negative, -1");
    JupiterAssertions.assertEquals("", StringUtils.repeat("foo", -1));
  }

  @Test
  public void testLpad()
  {
    String lpad = StringUtils.lpad("abc", 7, "de");
    JupiterAssertions.assertEquals("dedeabc", lpad);

    lpad = StringUtils.lpad("abc", 6, "de");
    JupiterAssertions.assertEquals("dedabc", lpad);

    lpad = StringUtils.lpad("abc", 2, "de");
    JupiterAssertions.assertEquals("ab", lpad);

    lpad = StringUtils.lpad("abc", 0, "de");
    JupiterAssertions.assertEquals("", lpad);

    lpad = StringUtils.lpad("abc", -1, "de");
    JupiterAssertions.assertEquals("", lpad);

    lpad = StringUtils.lpad("abc", 10, "");
    JupiterAssertions.assertEquals("abc", lpad);

    lpad = StringUtils.lpad("abc", 1, "");
    JupiterAssertions.assertEquals("a", lpad);
  }

  @Test
  public void testRpad()
  {
    String rpad = StringUtils.rpad("abc", 7, "de");
    JupiterAssertions.assertEquals("abcdede", rpad);

    rpad = StringUtils.rpad("abc", 6, "de");
    JupiterAssertions.assertEquals("abcded", rpad);

    rpad = StringUtils.rpad("abc", 2, "de");
    JupiterAssertions.assertEquals("ab", rpad);

    rpad = StringUtils.rpad("abc", 0, "de");
    JupiterAssertions.assertEquals("", rpad);

    rpad = StringUtils.rpad("abc", -1, "de");
    JupiterAssertions.assertEquals("", rpad);

    rpad = StringUtils.rpad("abc", 10, "");
    JupiterAssertions.assertEquals("abc", rpad);

    rpad = StringUtils.rpad("abc", 1, "");
    JupiterAssertions.assertEquals("a", rpad);
  }

  @Test
  public void testChop()
  {
    JupiterAssertions.assertEquals("foo", StringUtils.chop("foo", 5));
    JupiterAssertions.assertEquals("fo", StringUtils.chop("foo", 2));
    JupiterAssertions.assertEquals("", StringUtils.chop("foo", 0));
    JupiterAssertions.assertEquals("smile 🙂 for", StringUtils.chop("smile 🙂 for the camera", 14));
    JupiterAssertions.assertEquals("smile 🙂", StringUtils.chop("smile 🙂 for the camera", 10));
    JupiterAssertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 9));
    JupiterAssertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 8));
    JupiterAssertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 7));
    JupiterAssertions.assertEquals("smile ", StringUtils.chop("smile 🙂 for the camera", 6));
    JupiterAssertions.assertEquals("smile", StringUtils.chop("smile 🙂 for the camera", 5));
  }

  @Test
  public void testFastLooseChop()
  {
    JupiterAssertions.assertEquals("foo", StringUtils.fastLooseChop("foo", 5));
    JupiterAssertions.assertEquals("fo", StringUtils.fastLooseChop("foo", 2));
    JupiterAssertions.assertEquals("", StringUtils.fastLooseChop("foo", 0));
    JupiterAssertions.assertEquals("smile 🙂 for", StringUtils.fastLooseChop("smile 🙂 for the camera", 12));
    JupiterAssertions.assertEquals("smile 🙂 ", StringUtils.fastLooseChop("smile 🙂 for the camera", 9));
    JupiterAssertions.assertEquals("smile 🙂", StringUtils.fastLooseChop("smile 🙂 for the camera", 8));
    JupiterAssertions.assertEquals("smile \uD83D", StringUtils.fastLooseChop("smile 🙂 for the camera", 7));
    JupiterAssertions.assertEquals("smile ", StringUtils.fastLooseChop("smile 🙂 for the camera", 6));
    JupiterAssertions.assertEquals("smile", StringUtils.fastLooseChop("smile 🙂 for the camera", 5));
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

        JupiterAssertions.assertEquals(
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

        JupiterAssertions.assertEquals(
            StringUtils.format(
                "compareUtf8UsingJavaStringOrdering(byte[]) (actual) "
                + "matches compareJavaString (expected) for [%s] vs [%s]",
                string1,
                string2
            ),
            (int) Math.signum(compareJavaString),
            (int) Math.signum(compareByteArrayUtf8UsingJavaStringOrdering)
        );

        JupiterAssertions.assertEquals(
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

  @Test()
  public void testNonStrictFormatWithNullMessage()
  {
    JupiterAssertions.assertThrows(NullPointerException.class, () -> StringUtils.nonStrictFormat(null, 1, 2));
  }

  @Test
  public void testNonStrictFormatWithStringContainingPercent()
  {
    JupiterAssertions.assertEquals(
        "some string containing % %s %d %f",
        StringUtils.nonStrictFormat("%s", "some string containing % %s %d %f")
    );
  }
}
