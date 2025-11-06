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

package org.apache.druid.segment.serde;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;

public class DictionarySerdeHelper
{
  public static final int NO_FLAGS = 0;

  public enum Feature
  {
    MULTI_VALUE,
    MULTI_VALUE_V3,
    NO_BITMAP_INDEX,
    COMPRESSED,
    CONFIGURABLE_BITMAP_INDEX;

    public boolean isSet(int flags)
    {
      return (getMask() & flags) != 0;
    }

    public int getMask()
    {
      return (1 << ordinal());
    }

    private static void requireAtMostOneSet(int flags, Feature feat1, Feature feat2)
    {
      if (feat1.isSet(flags) && feat2.isSet(flags)) {
        throw new IAE(
            "Incompatible feature: feature[%s] and feature[%s], got: [%b] and [%b]\"",
            feat1,
            feat2,
            feat1.isSet(flags),
            feat2.isSet(flags)
        );
      }
    }

    private static void requireFlagDependency(int flags, Feature feat1, Feature feat2)
    {
      if (feat1.isSet(flags) && !feat2.isSet(flags)) {
        throw new IAE(
            "Feature[%s] requires feature[%s] set, got: [%b] and [%b]",
            feat1,
            feat2,
            feat1.isSet(flags),
            feat2.isSet(flags)
        );
      }
    }

    public static int validateFeatures(int flags)
    {
      int validMask = (1 << Feature.values().length) - 1;
      Preconditions.checkArgument(
          (flags & ~validMask) == 0,
          StringUtils.format("Invalid flag bits set: 0x%X (valid mask: 0x%X)", flags, validMask)
      );
      requireAtMostOneSet(flags, Feature.MULTI_VALUE, Feature.MULTI_VALUE_V3);
      requireFlagDependency(flags, Feature.MULTI_VALUE_V3, Feature.COMPRESSED);
      return flags;
    }
  }

  public enum VERSION
  {
    // plain version
    UNCOMPRESSED_SINGLE_VALUE,  // 0x0
    UNCOMPRESSED_MULTI_VALUE,   // 0x1

    // starting from verion 0x2, we would use an int as feature flags
    FLAG_BASED_FORCE_COMPRESSION,                 // 0x2
    FLAG_BASED;    // 0x3

    public static VERSION fromByte(byte b)
    {
      final VERSION[] values = VERSION.values();
      Preconditions.checkArgument(b < values.length, "Unsupported dictionary column version[%s]", b);
      return values[b];
    }

    public boolean isFlagBased()
    {
      return this.compareTo(FLAG_BASED_FORCE_COMPRESSION) >= 0;
    }

    public int getFlags(ByteBuffer buffer)
    {
      if (isFlagBased()) {
        int flags = buffer.getInt();
        if (equals(FLAG_BASED_FORCE_COMPRESSION)) {
          flags = flags | Feature.COMPRESSED.getMask();
        }
        return Feature.validateFeatures(flags);
      } else if (equals(UNCOMPRESSED_MULTI_VALUE)) {
        return Feature.MULTI_VALUE.getMask();
      } else {
        return NO_FLAGS; // plain old UNCOMPRESSED_SINGLE_VALUE, no feature.
      }
    }

    public static VERSION compatibleVersion(int flags)
    {
      if (flags == 0) {
        return VERSION.UNCOMPRESSED_SINGLE_VALUE;
      } else if (flags == Feature.MULTI_VALUE.getMask()) {
        return VERSION.UNCOMPRESSED_MULTI_VALUE;
      } else if (Feature.COMPRESSED.isSet(flags)) {
        return VERSION.FLAG_BASED_FORCE_COMPRESSION;
      }
      return VERSION.FLAG_BASED;
    }

    public byte asByte()
    {
      return (byte) this.ordinal();
    }
  }

  public static boolean hasMultiValue(final int flags)
  {
    return DictionarySerdeHelper.Feature.MULTI_VALUE.isSet(flags) || Feature.MULTI_VALUE_V3.isSet(flags);
  }
}
