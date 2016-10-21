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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.druid.java.util.common.logger.Logger;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.IllegalFormatException;

/**
 * As of right now (Dec 2014) the JVM is optimized around String charset variablse instead of Charset passing.
 */
public class StringUtils
{
  @Deprecated // Charset parameters to String are currently slower than the charset's string name
  public static final Charset UTF8_CHARSET = Charsets.UTF_8;
  public static final String UTF8_STRING = com.google.common.base.Charsets.UTF_8.toString();

  public static String fromUtf8(final byte[] bytes)
  {
    try {
      return new String(bytes, UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw Throwables.propagate(e);
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
    return fromUtf8(buffer, buffer.remaining());
  }

  public static byte[] toUtf8(final String string)
  {
    try {
      return string.getBytes(UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  public static String safeFormat(String message, Object... formatArgs)
  {
    if(formatArgs == null || formatArgs.length == 0) {
      return message;
    }
    try {
      return String.format(message, formatArgs);
    }
    catch (IllegalFormatException e) {
      StringBuilder bob = new StringBuilder(message);
      for (Object formatArg : formatArgs) {
        bob.append("; ").append(formatArg);
      }
      return bob.toString();
    }
  }
}
