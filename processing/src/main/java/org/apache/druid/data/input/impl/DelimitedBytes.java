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

package org.apache.druid.data.input.impl;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility function for {@link DelimitedInputFormat}.
 */
public class DelimitedBytes
{
  /**
   * Parameter for {@link #split(byte[], byte, int)} signifying that we do not know the expected number of fields.
   */
  public static final int UNKNOWN_FIELD_COUNT = -1;

  /**
   * Split UTF-8 bytes by a particular delimiter. When {@link NullHandling#sqlCompatible()}, empty parts are
   * returned as nulls. When {@link NullHandling#replaceWithDefault()}, empty parts are returned as empty strings.
   *
   * @param bytes         utf-8 bytes
   * @param delimiter     the delimiter
   * @param numFieldsHint expected number of fields, or {@link #UNKNOWN_FIELD_COUNT}
   */
  public static List<String> split(final byte[] bytes, final byte delimiter, final int numFieldsHint)
  {
    final List<String> out = numFieldsHint == UNKNOWN_FIELD_COUNT ? new ArrayList<>() : new ArrayList<>(numFieldsHint);

    int start = 0;
    int position = 0;

    while (position < bytes.length) {
      if (bytes[position] == delimiter) {
        final String s = StringUtils.fromUtf8(bytes, start, position - start);
        out.add(s.isEmpty() && NullHandling.sqlCompatible() ? null : s);
        start = position + 1;
      }

      position++;
    }

    final String s = StringUtils.fromUtf8(bytes, start, position - start);
    out.add(s.isEmpty() && NullHandling.sqlCompatible() ? null : s);
    return out;
  }
}
