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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.DimensionDictionarySelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class HllSketchBuildUtil
{
  public static void updateSketch(final HllSketch sketch, final StringEncoding stringEncoding, final Object value)
  {
    if (value instanceof Integer || value instanceof Long) {
      sketch.update(((Number) value).longValue());
    } else if (value instanceof Float || value instanceof Double) {
      sketch.update(((Number) value).doubleValue());
    } else if (value instanceof String) {
      updateSketchWithString(sketch, stringEncoding, (String) value);
    } else if (value instanceof List) {
      // noinspection rawtypes
      for (Object entry : (List) value) {
        if (entry != null) {
          final String asString = entry.toString();
          if (!NullHandling.isNullOrEquivalent(asString)) {
            updateSketchWithString(sketch, stringEncoding, asString);
          }
        }
      }
    } else if (value instanceof char[]) {
      sketch.update((char[]) value);
    } else if (value instanceof byte[]) {
      sketch.update((byte[]) value);
    } else if (value instanceof int[]) {
      sketch.update((int[]) value);
    } else if (value instanceof long[]) {
      sketch.update((long[]) value);
    } else {
      throw new IAE("Unsupported type " + value.getClass());
    }
  }

  public static void updateSketchWithDictionarySelector(
      final HllSketch sketch,
      final StringEncoding stringEncoding,
      final DimensionDictionarySelector selector,
      final int id
  )
  {
    if (stringEncoding == StringEncoding.UTF8 && selector.supportsLookupNameUtf8()) {
      final ByteBuffer buf = selector.lookupNameUtf8(id);

      if (buf != null) {
        sketch.update(buf);
      }
    } else {
      final String s = NullHandling.nullToEmptyIfNeeded(selector.lookupName(id));
      updateSketchWithString(sketch, stringEncoding, s);
    }
  }

  private static void updateSketchWithString(
      final HllSketch sketch,
      final StringEncoding stringEncoding,
      @Nullable final String value
  )
  {
    if (value == null) {
      return;
    }

    switch (stringEncoding) {
      case UTF8:
        sketch.update(StringUtils.toUtf8(value));
        break;
      case UTF16LE:
        sketch.update(value.toCharArray());
        break;
      default:
        throw new UOE("Unsupported string encoding [%s]", stringEncoding);
    }
  }
}
