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

package org.apache.druid.frame;

import org.apache.druid.frame.field.TransformUtils;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.frame.write.columnar.ColumnarFrameWriter;

import javax.annotation.Nullable;

public enum FrameType
{
  /**
   * Columnar frames, written by {@link ColumnarFrameWriter}.
   */
  COLUMNAR((byte) 0x11, false),

  /**
   * Row-based frames (version 1), written by {@link RowBasedFrameWriter}. Obsolete.
   *
   * Differs from {@link #ROW_BASED_V2} in how floating-point numbers are serialized. This one uses
   * {@link TransformUtils#transformFromDoubleLegacy(double)} and
   * {@link TransformUtils#transformFromFloatLegacy(float)}, which sort incorrectly.
   */
  ROW_BASED_V1((byte) 0x12, true),

  /**
   * Row-based frames (version 2), written by {@link RowBasedFrameWriter}.
   */
  ROW_BASED_V2((byte) 0x13, true);

  /**
   * Returns the frame type for a particular version byte, or null if the version byte is unrecognized.
   */
  @Nullable
  public static FrameType forVersion(final byte versionByte)
  {
    for (final FrameType type : values()) {
      if (type.version() == versionByte) {
        return type;
      }
    }

    return null;
  }

  /**
   * Returns the latest row-based frame type.
   */
  public static FrameType latestRowBased()
  {
    return ROW_BASED_V2;
  }

  /**
   * Returns the latest columnar frame type.
   */
  public static FrameType latestColumnar()
  {
    return COLUMNAR;
  }

  private final byte versionByte;
  private final boolean rowBased;

  FrameType(byte versionByte, boolean rowBased)
  {
    this.versionByte = versionByte;
    this.rowBased = rowBased;
  }

  public byte version()
  {
    return versionByte;
  }

  /**
   * Whether this is a row-based type, written by {@link RowBasedFrameWriter}.
   */
  public boolean isRowBased()
  {
    return rowBased;
  }

  /**
   * Whether this is a columnar type, written by {@link ColumnarFrameWriter}.
   */
  public boolean isColumnar()
  {
    return !rowBased;
  }
}
