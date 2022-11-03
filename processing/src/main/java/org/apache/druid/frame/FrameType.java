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

import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

public enum FrameType
{
  COLUMNAR((byte) 0x11),
  ROW_BASED((byte) 0x12);

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

  private final byte versionByte;

  FrameType(final byte magicByte)
  {
    this.versionByte = magicByte;
  }

  public byte version()
  {
    return versionByte;
  }

  public Frame ensureType(final Frame frame)
  {
    if (frame == null) {
      throw new NullPointerException("Null frame");
    }

    if (frame.type() != this) {
      throw new ISE("Frame type must be [%s], but was [%s]", this, frame.type());
    }

    return frame;
  }
}
