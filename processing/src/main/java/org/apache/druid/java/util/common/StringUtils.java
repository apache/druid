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
import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
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

  /**
   * Compares two Java Strings in Unicode code-point order.
   *
   * Order is consistent with {@link #compareUtf8(byte[], byte[])}, but is not consistent with
   * {@link String#compareTo(String)}.
   */
  public static int compareUnicode(final String a, final String b)
  {
    final int commonLength = Math.min(a.length(), b.length());

    for (int i = 0; i < commonLength; i++) {
      int char1 = a.charAt(i) & 0xFFFF; // Unsigned
      int char2 = b.charAt(i) & 0xFFFF; // Unsigned

      if (char1 != char2 && char1 >= 0xd800 && char2 >= 0xd800) {
        // Fixup logic for code units at or above the surrogate range, based on logic described at
        // https://www.icu-project.org/docs/papers/utf16_code_point_order.html.
        //
        // If both code units are at or above the surrogate range (>= 0xd800) then adjust non-surrogates (legitimate
        // single-code-unit characters) to be below the surrogate range, so they compare earlier than surrogates.

        if (!Character.isSurrogate((char) char1)) {
          char1 -= 0x2800;
        }

        if (!Character.isSurrogate((char) char2)) {
          char2 -= 0x2800;
        }
      }

      final int cmp = char1 - char2;
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(a.length(), b.length());
  }

  /**
   * Compares two UTF-8 byte strings in Unicode code-point order.
   *
   * Equivalent to a comparison of the two byte arrays as if they were unsigned bytes.
   *
   * Order is consistent with {@link #compareUnicode(String, String)}, but is not consistent with
   * {@link String#compareTo(String)}. For an ordering consistent with {@link String#compareTo(String)}, use
   * {@link #compareUtf8UsingJavaStringOrdering(byte[], byte[])} instead.
   */
  public static int compareUtf8(final byte[] a, final byte[] b)
  {
    final int commonLength = Math.min(a.length, b.length);

    for (int i = 0; i < commonLength; i++) {
      final byte byte1 = a[i];
      final byte byte2 = b[i];
      final int cmp = (byte1 & 0xFF) - (byte2 & 0xFF); // Unsigned comparison
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(a.length, b.length);
  }

  /**
   * Compares two UTF-8 byte strings in UTF-16 code-unit order.
   *
   * Order is consistent with {@link String#compareTo(String)}, but is not consistent with
   * {@link #compareUnicode(String, String)} or {@link #compareUtf8(byte[], byte[])}.
   */
  public static int compareUtf8UsingJavaStringOrdering(final byte[] a, final byte[] b)
  {
    final int commonLength = Math.min(a.length, b.length);

    for (int i = 0; i < commonLength; i++) {
      final int cmp = compareUtf8UsingJavaStringOrdering(a[i], b[i]);
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(a.length, b.length);
  }

  /**
   * Compares two UTF-8 byte strings in UTF-16 code-unit order.
   *
   * Order is consistent with {@link String#compareTo(String)}, but is not consistent with
   * {@link #compareUnicode(String, String)} or {@link #compareUtf8(byte[], byte[])}.
   */
  public static int compareUtf8UsingJavaStringOrdering(
      final ByteBuffer buf1,
      final int position1,
      final int length1,
      final ByteBuffer buf2,
      final int position2,
      final int length2
  )
  {
    final int commonLength = Math.min(length1, length2);

    for (int i = 0; i < commonLength; i++) {
      final int cmp = compareUtf8UsingJavaStringOrdering(buf1.get(position1 + i), buf2.get(position2 + i));
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(length1, length2);
  }

  /**
   * Compares two bytes from UTF-8 strings in such a way that the entire byte arrays are compared in UTF-16
   * code-unit order.
   *
   * Compatible with {@link #compareUtf8UsingJavaStringOrdering(byte[], byte[])} and
   * {@link #compareUtf8UsingJavaStringOrdering(ByteBuffer, int, int, ByteBuffer, int, int)}.
   */
  public static int compareUtf8UsingJavaStringOrdering(byte byte1, byte byte2)
  {
    // Treat as unsigned bytes.
    int ubyte1 = byte1 & 0xFF;
    int ubyte2 = byte2 & 0xFF;

    if (ubyte1 != ubyte2 && ubyte1 >= 0xEE && ubyte2 >= 0xEE) {
      // Fixup logic for lead bytes for U+E000 ... U+FFFF, based on logic described at
      // https://www.icu-project.org/docs/papers/utf16_code_point_order.html.
      //
      // Move possible lead bytes for this range (0xEE and 0xEF) above all other bytes, so they compare later.

      if (ubyte1 == 0xEE || ubyte1 == 0xEF) {
        ubyte1 += 0xFF;
      }

      if (ubyte2 == 0xEE || ubyte2 == 0xEF) {
        ubyte2 += 0xFF;
      }
    }

    return ubyte1 - ubyte2;
  }

  public static String fromUtf8(final byte[] bytes)
  {
    return fromUtf8(bytes, 0, bytes.length);
  }

  public static String fromUtf8(final byte[] bytes, int offset, int length)
  {
    try {
      return new String(bytes, offset, length, UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw new RuntimeException(e);
    }
  }

  /**
   * Decodes a UTF-8 String from {@code numBytes} bytes starting at the current position of a buffer.
   * Advances the position of the buffer by {@code numBytes}.
   */
  public static String fromUtf8(final ByteBuffer buffer, final int numBytes)
  {
    final byte[] bytes = new byte[numBytes];
    buffer.get(bytes);
    return fromUtf8(bytes);
  }

  /**
   * Decodes a UTF-8 string from the remaining bytes of a non-null buffer.
   * Advances the position of the buffer by {@link ByteBuffer#remaining()}.
   *
   * Use {@link #fromUtf8Nullable(ByteBuffer)} if the buffer might be null.
   */
  public static String fromUtf8(final ByteBuffer buffer)
  {
    return StringUtils.fromUtf8(buffer, buffer.remaining());
  }

  /**
   * If buffer is Decodes a UTF-8 string from the remaining bytes of a buffer.
   * Advances the position of the buffer by {@link ByteBuffer#remaining()}.
   *
   * If the value is null, this method returns null. If the buffer will never be null, use {@link #fromUtf8(ByteBuffer)}
   * instead.
   */
  @Nullable
  public static String fromUtf8Nullable(@Nullable final ByteBuffer buffer)
  {
    if (buffer == null) {
      return null;
    }
    return StringUtils.fromUtf8(buffer, buffer.remaining());
  }

  /**
   * Converts a string to a UTF-8 byte array.
   *
   * @throws NullPointerException if "string" is null
   */
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

  /**
   * Converts a string to UTF-8 bytes, returning them as a newly-allocated on-heap {@link ByteBuffer}.
   * If "string" is null, returns null.
   */
  @Nullable
  public static ByteBuffer toUtf8ByteBuffer(@Nullable final String string)
  {
    return string == null ? null : ByteBuffer.wrap(toUtf8(string));
  }

  /**
   * Encodes "string" into the buffer "byteBuffer", using no more than the number of bytes remaining in the buffer.
   * Will only encode whole characters. The byteBuffer's position and limit may be changed during operation, but will
   * be reset before this method call ends.
   *
   * @return the number of bytes written, which may be shorter than the full encoded string length if there
   * is not enough room in the output buffer.
   */
  public static int toUtf8WithLimit(final String string, final ByteBuffer byteBuffer)
  {
    final CharsetEncoder encoder = StandardCharsets.UTF_8
        .newEncoder()
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE);

    final int originalPosition = byteBuffer.position();
    final int originalLimit = byteBuffer.limit();
    final int maxBytes = byteBuffer.remaining();

    try {
      final char[] chars = string.toCharArray();
      final CharBuffer charBuffer = CharBuffer.wrap(chars);

      // No reason to look at the CoderResult from the "encode" call; we can tell the number of transferred characters
      // by looking at the output buffer's position.
      encoder.encode(charBuffer, byteBuffer, true);

      final int bytesWritten = byteBuffer.position() - originalPosition;

      assert bytesWritten <= maxBytes;
      return bytesWritten;
    }
    finally {
      byteBuffer.position(originalPosition);
      byteBuffer.limit(originalLimit);
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

  /**
   * Encodes a string "s" for insertion into a format string.
   *
   * Returns null if the input is null.
   */
  @Nullable
  public static String encodeForFormat(@Nullable final String s)
  {
    if (s == null) {
      return null;
    } else {
      return StringUtils.replaceChar(s, '%', "%%");
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
   *
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

  public static String maybeRemoveLeadingSlash(String s)
  {
    return s != null && s.startsWith("/") ? s.substring(1) : s;
  }

  public static String maybeRemoveTrailingSlash(String s)
  {
    return s != null && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  public static String maybeAppendTrailingSlash(String s)
  {
    return s != null && !s.endsWith("/") ? s + "/" : s;
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
   *
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
   *
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
   *
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
   *
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
   *
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
   * This function is migrated from flink's scala function with minor refactor
   * https://github.com/apache/flink/blob/master/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/runtime/functions/ScalarFunctions.scala
   * - Modified to handle empty pad string.
   * - Padding of negative length return an empty string.
   *
   * @param base The base string to be padded
   * @param len  The length of padded string
   * @param pad  The pad string
   *
   * @return the string left-padded with pad to a length of len or null if the pad is empty or the len is less than 0.
   */
  @Nonnull
  public static String lpad(@Nonnull String base, int len, @Nonnull String pad)
  {
    if (len <= 0) {
      return "";
    }

    // The length of the padding needed
    int pos = Math.max(len - base.length(), 0);

    // short-circuit if there is no pad and we need to add a padding
    if (pos > 0 && pad.isEmpty()) {
      return base;
    }

    char[] data = new char[len];

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
   * This function is migrated from flink's scala function with minor refactor
   * https://github.com/apache/flink/blob/master/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/runtime/functions/ScalarFunctions.scala
   * - Modified to handle empty pad string.
   * - Modified to only copy the pad string if needed (this implementation mimics lpad).
   * - Padding of negative length return an empty string.
   *
   * @param base The base string to be padded
   * @param len  The length of padded string
   * @param pad  The pad string
   *
   * @return the string right-padded with pad to a length of len or null if the pad is empty or the len is less than 0.
   */
  @Nonnull
  public static String rpad(@Nonnull String base, int len, @Nonnull String pad)
  {
    if (len <= 0) {
      return "";
    }

    // The length of the padding needed
    int paddingLen = Math.max(len - base.length(), 0);

    // short-circuit if there is no pad and we need to add a padding
    if (paddingLen > 0 && pad.isEmpty()) {
      return base;
    }

    char[] data = new char[len];


    // Copy the padding
    for (int i = len - paddingLen; i < len; i += pad.length()) {
      for (int j = 0; j < pad.length() && i + j < data.length; j++) {
        data[i + j] = pad.charAt(j);
      }
    }

    // Copy the base
    for (int i = 0; i < len && i < base.length(); i++) {
      data[i] = base.charAt(i);
    }

    return new String(data);
  }

  /**
   * Returns the string truncated to maxBytes.
   * If given string input is shorter than maxBytes, then it remains the same.
   *
   * @param s        The input string to possibly be truncated
   * @param maxBytes The max bytes that string input will be truncated to
   *
   * @return the string after truncated to maxBytes
   */
  @Nullable
  public static String chop(@Nullable final String s, final int maxBytes)
  {
    if (s == null) {
      return null;
    } else {
      // Shorten firstValue to what could fit in maxBytes as UTF-8.
      final byte[] bytes = new byte[maxBytes];
      final int len = StringUtils.toUtf8WithLimit(s, ByteBuffer.wrap(bytes));
      return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }
  }

  /**
   * Shorten "s" to "maxBytes" chars. Fast and loose because these are *chars* not *bytes*. Use
   * {@link #chop(String, int)} for slower, but accurate chopping.
   */
  @Nullable
  public static String fastLooseChop(@Nullable final String s, final int maxBytes)
  {
    if (s == null || s.length() <= maxBytes) {
      return s;
    } else {
      return s.substring(0, maxBytes);
    }
  }

  public static String getResource(Object ref, String resource)
  {
    try {
      InputStream is = ref.getClass().getResourceAsStream(resource);
      if (is == null) {
        throw new ISE("Resource not found: [%s]", resource);
      }
      return IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "Cannot load resource: [%s]", resource);
    }
  }
}
