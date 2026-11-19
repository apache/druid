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

package org.apache.druid.jdbc;

import java.util.Locale;

/**
 * Small string helpers. This driver deliberately avoids depending on other Druid modules, so it
 * carries its own minimal utilities rather than reusing {@code org.apache.druid.java.util.common.StringUtils}.
 */
public class StringUtils
{
  private StringUtils()
  {
    // No instantiation.
  }

  /**
   * Formats a string using {@link Locale#ENGLISH}, so results do not depend on the JVM default
   * locale. Prefer this (and format strings generally) over string concatenation for messages.
   */
  public static String format(final String format, final Object... args)
  {
    return String.format(Locale.ENGLISH, format, args);
  }

  /**
   * Equivalent to {@link String#replace(CharSequence, CharSequence)}.
   */
  @SuppressForbidden(reason = "String#replace")
  public static String replace(final String source, final String target, final String replacement)
  {
    return source.replace(target, replacement);
  }
}
