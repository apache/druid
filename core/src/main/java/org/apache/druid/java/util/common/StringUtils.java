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

import com.google.common.base.Strings;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.IllegalFormatException;
import java.util.Locale;

/**
 * As of OpenJDK / Oracle JDK 8, the JVM is optimized around String charset variable instead of Charset passing, that
 * is exploited in {@link #toUtf8(String)} and {@link #fromUtf8(byte[])}.
 */
public class StringUtils
{
  public static final byte[] EMPTY_BYTES = new byte[0];
  @Deprecated // Charset parameters to String are currently slower than the charset's string name
  public static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
  public static final String UTF8_STRING = StandardCharsets.UTF_8.toString();
  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
  private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

  // should be used only for estimation
  // returns the same result with StringUtils.fromUtf8(value).length for valid string values
  // does not check validity of format and returns over-estimated result for invalid string (see UT)
  public static int estimatedBinaryLengthAsUTF8(String value)
  {
    int length = 0;
    for (int i = 0; i < value.length(); i++) {
      char var10 = value.charAt(i);
      if (var10 < 0x80) {
        length += 1;
      } else if (var10 < 0x800) {
        length += 2;
      } else if (Character.isSurrogate(var10)) {
        length += 4;
        i++;
      } else {
        length += 3;
      }
    }
    return length;
  }

  public static byte[] toUtf8WithNullToEmpty(final String string)
  {
    return string == null ? EMPTY_BYTES : toUtf8(string);
  }

  public static String fromUtf8(final byte[] bytes)
  {
    try {
      return new String(bytes, UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw new RuntimeException(e);
    }
  }

  public static String fromUtf8(final ByteBuffer buffer, final int numBytes)
  {
    final byte[] bytes = new byte[numBytes];
    buffer.get(bytes);
    return fromUtf8(bytes);
  }

  public static String fromUtf8(final ByteBuffer buffer)
  {
    return StringUtils.fromUtf8(buffer, buffer.remaining());
  }

  public static byte[] toUtf8(final String string)
  {
    try {
      return string.getBytes(UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public static byte[] toUtf8Nullable(@Nullable final String string)
  {
    if (string == null) {
      return null;
    }
    return toUtf8(string);
  }

  /**
   * Equivalent of String.format(Locale.ENGLISH, message, formatArgs).
   */
  public static String format(String message, Object... formatArgs)
  {
    return String.format(Locale.ENGLISH, message, formatArgs);
  }

  /**
   * Formats the string as {@link #format(String, Object...)}, but instead of failing on illegal format, returns the
   * concatenated format string and format arguments. Should be used for unimportant formatting like logging,
   * exception messages, typically not directly.
   */
  public static String nonStrictFormat(String message, Object... formatArgs)
  {
    if (formatArgs == null || formatArgs.length == 0) {
      return message;
    }
    try {
      return String.format(Locale.ENGLISH, message, formatArgs);
    }
    catch (IllegalFormatException e) {
      StringBuilder bob = new StringBuilder(message);
      for (Object formatArg : formatArgs) {
        bob.append("; ").append(formatArg);
      }
      return bob.toString();
    }
  }

  public static String toLowerCase(String s)
  {
    return s.toLowerCase(Locale.ENGLISH);
  }

  public static String toUpperCase(String s)
  {
    return s.toUpperCase(Locale.ENGLISH);
  }

  /**
   * Encodes a String in application/x-www-form-urlencoded format, with one exception:
   * "+" in the encoded form is replaced with "%20".
   *
   * application/x-www-form-urlencoded encodes spaces as "+", but we use this to encode non-form data as well.
   *
   * @param s String to be encoded
   * @return application/x-www-form-urlencoded format encoded String, but with "+" replaced with "%20".
   */
  @Nullable
  public static String urlEncode(@Nullable String s)
  {
    if (s == null) {
      return null;
    }

    try {
      return StringUtils.replace(URLEncoder.encode(s, "UTF-8"), "+", "%20");
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public static String urlDecode(String s)
  {
    if (s == null) {
      return null;
    }

    try {
      return URLDecoder.decode(s, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Removes all occurrences of the given char from the given string. This method is an optimal version of
   * {@link String#replace(CharSequence, CharSequence) s.replace("c", "")}.
   */
  public static String removeChar(String s, char c)
  {
    int pos = s.indexOf(c);
    if (pos < 0) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length() - 1);
    int prevPos = 0;
    do {
      sb.append(s, prevPos, pos);
      prevPos = pos + 1;
      pos = s.indexOf(c, pos + 1);
    } while (pos > 0);
    sb.append(s, prevPos, s.length());
    return sb.toString();
  }

  /**
   * Replaces all occurrences of the given char in the given string with the given replacement string. This method is an
   * optimal version of {@link String#replace(CharSequence, CharSequence) s.replace("c", replacement)}.
   */
  public static String replaceChar(String s, char c, String replacement)
  {
    int pos = s.indexOf(c);
    if (pos < 0) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length() - 1 + replacement.length());
    int prevPos = 0;
    do {
      sb.append(s, prevPos, pos);
      sb.append(replacement);
      prevPos = pos + 1;
      pos = s.indexOf(c, pos + 1);
    } while (pos > 0);
    sb.append(s, prevPos, s.length());
    return sb.toString();
  }

  /**
   * Replaces all occurrences of the given target substring in the given string with the given replacement string. This
   * method is an optimal version of {@link String#replace(CharSequence, CharSequence) s.replace(target, replacement)}.
   */
  public static String replace(String s, String target, String replacement)
  {
    // String.replace() is suboptimal in JDK8, but is fixed in JDK9+. When the minimal JDK version supported by Druid is
    // JDK9+, the implementation of this method should be replaced with simple delegation to String.replace(). However,
    // the method should still be prohibited to use in all other places except this method body, because it's easy to
    // suboptimally call String.replace("a", "b"), String.replace("a", ""), String.replace("a", "abc"), which have
    // better alternatives String.replace('a', 'b'), removeChar() and replaceChar() respectively.
    int pos = s.indexOf(target);
    if (pos < 0) {
      return s;
    }
    int sLength = s.length();
    int targetLength = target.length();
    // This is needed to work correctly with empty target string and mimic String.replace() behavior
    int searchSkip = Math.max(targetLength, 1);
    StringBuilder sb = new StringBuilder(sLength - targetLength + replacement.length());
    int prevPos = 0;
    do {
      sb.append(s, prevPos, pos);
      sb.append(replacement);
      prevPos = pos + targetLength;
      // Break from the loop if the target is empty
      if (pos == sLength) {
        break;
      }
      pos = s.indexOf(target, pos + searchSkip);
    } while (pos > 0);
    sb.append(s, prevPos, sLength);
    return sb.toString();
  }

  /**
   * Returns the given string if it is non-null; the empty string otherwise.
   * This method should only be used at places where null to empty conversion is
   * irrelevant to null handling of the data.
   *
   * @param string the string to test and possibly return
   *
   * @return {@code string} itself if it is non-null; {@code ""} if it is null
   */
  public static String nullToEmptyNonDruidDataString(@Nullable String string)
  {
    //CHECKSTYLE.OFF: Regexp
    return Strings.nullToEmpty(string);
    //CHECKSTYLE.ON: Regexp
  }

  /**
   * Returns the given string if it is nonempty; {@code null} otherwise.
   * This method should only be used at places where null to empty conversion is
   * irrelevant to null handling of the data.
   *
   * @param string the string to test and possibly return
   *
   * @return {@code string} itself if it is nonempty; {@code null} if it is
   * empty or null
   */
  @Nullable
  public static String emptyToNullNonDruidDataString(@Nullable String string)
  {
    //CHECKSTYLE.OFF: Regexp
    return Strings.emptyToNull(string);
    //CHECKSTYLE.ON: Regexp
  }

  /**
   * Convert an input to base 64 and return the utf8 string of that byte array
   *
   * @param input The string to convert to base64
   * @return the base64 of the input in string form
   */
  public static String utf8Base64(String input)
  {
    return fromUtf8(encodeBase64(toUtf8(input)));
  }

  /**
   * Convert an input byte array into a newly-allocated byte array using the {@link Base64} encoding scheme
   *
   * @param input The byte array to convert to base64
   * @return the base64 of the input in byte array form
   */
  public static byte[] encodeBase64(byte[] input)
  {
    return BASE64_ENCODER.encode(input);
  }

  /**
   * Convert an input byte array into a string using the {@link Base64} encoding scheme
   *
   * @param input The byte array to convert to base64
   * @return the base64 of the input in string form
   */
  public static String encodeBase64String(byte[] input)
  {
    return BASE64_ENCODER.encodeToString(input);
  }

  /**
   * Decode an input byte array using the {@link Base64} encoding scheme and return a newly-allocated byte array
   *
   * @param input The byte array to decode from base64
   * @return a newly-allocated byte array
   */
  public static byte[] decodeBase64(byte[] input)
  {
    return BASE64_DECODER.decode(input);
  }

  /**
   * Decode an input string using the {@link Base64} encoding scheme and return a newly-allocated byte array
   *
   * @param input The string to decode from base64
   * @return a newly-allocated byte array
   */
  public static byte[] decodeBase64String(String input)
  {
    return BASE64_DECODER.decode(input);
  }

  /**
   * Returns a string whose value is the concatenation of the
   * string {@code s} repeated {@code count} times.
   * <p>
   * If count or length is zero then the empty string is returned.
   * <p>
   * This method may be used to create space padding for
   * formatting text or zero padding for formatting numbers.
   *
   * @param count number of times to repeat
   *
   * @return A string composed of this string repeated
   * {@code count} times or the empty string if count
   * or length is zero.
   *
   * @throws IllegalArgumentException if the {@code count} is negative.
   * @link https://bugs.openjdk.java.net/browse/JDK-8197594
   */
  public static String repeat(String s, int count)
  {
    if (count < 0) {
      throw new IllegalArgumentException("count is negative, " + count);
    }
    if (count == 1) {
      return s;
    }
    byte[] value = s.getBytes(StandardCharsets.UTF_8);
    final int len = value.length;
    if (len == 0 || count == 0) {
      return "";
    }
    if (len == 1) {
      final byte[] single = new byte[count];
      Arrays.fill(single, value[0]);
      return new String(single, StandardCharsets.UTF_8);
    }
    if (Integer.MAX_VALUE / count < len) {
      throw new RuntimeException("The produced string is too large.");
    }
    final int limit = len * count;
    final byte[] multiple = new byte[limit];
    System.arraycopy(value, 0, multiple, 0, len);
    int copied = len;
    for (; copied < limit - copied; copied <<= 1) {
      System.arraycopy(multiple, 0, multiple, copied, copied);
    }
    System.arraycopy(multiple, 0, multiple, copied, limit - copied);
    return new String(multiple, StandardCharsets.UTF_8);
  }
   
  /**
   * Returns the string left-padded with the string pad to a length of len characters.
   * If str is longer than len, the return value is shortened to len characters.
   * Lpad and rpad functions are migrated from flink's scala function with minor refactor
   * https://github.com/apache/flink/blob/master/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/runtime/functions/ScalarFunctions.scala
   *
   * @param base The base string to be padded
   * @param len The length of padded string
   * @param pad The pad string
   * @return the string left-padded with pad to a length of len
   */
  public static String lpad(String base, Integer len, String pad)
  {
    if (len < 0) {
      return null;
    } else if (len == 0) {
      return "";
    }

    char[] data = new char[len];

    // The length of the padding needed
    int pos = Math.max(len - base.length(), 0);

    // Copy the padding
    for (int i = 0; i < pos; i += pad.length()) {
      for (int j = 0; j < pad.length() && j < pos - i; j++) {
        data[i + j] = pad.charAt(j);
      }
    }

    // Copy the base
    for (int i = 0; pos + i < len && i < base.length(); i++) {
      data[pos + i] = base.charAt(i);
    }

    return new String(data);
  }

  /**
   * Returns the string right-padded with the string pad to a length of len characters.
   * If str is longer than len, the return value is shortened to len characters. 
   *
   * @param base The base string to be padded
   * @param len The length of padded string
   * @param pad The pad string
   * @return the string right-padded with pad to a length of len
   */
  public static String rpad(String base, Integer len, String pad)
  {
    if (len < 0) {
      return null;
    } else if (len == 0) {
      return "";
    }

    char[] data = new char[len];

    int pos = 0;

    // Copy the base
    for ( ; pos < base.length() && pos < len; pos++) {
      data[pos] = base.charAt(pos);
    }

    // Copy the padding
    for ( ; pos < len; pos += pad.length()) {
      for (int i = 0; i < pad.length() && i < len - pos; i++) {
        data[pos + i] = pad.charAt(i);
      }
    }

    return new String(data);
  }

}
