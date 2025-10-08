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

package org.apache.druid.segment.data;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.file.SegmentFileMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedColumnarDoublesSuppliers
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte VERSION = 0x2;

  private CompressedColumnarDoublesSuppliers()
  {
  }

  /**
   * Reads a column from a {@link ByteBuffer}, possibly using additional secondary files from a
   * {@link SegmentFileMapper}.
   *
   * @param buffer     primary buffer to read from
   * @param order      byte order
   * @param fileMapper required for reading version 2 (multi-file) indexed. May be null if you know you are reading
   *                   a single-file column. Generally, this should only be null in tests, not production code.
   */
  public static Supplier<ColumnarDoubles> fromByteBuffer(
      ByteBuffer buffer,
      ByteOrder order,
      SegmentFileMapper fileMapper
  )
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      CompressionStrategy compression = CompressionStrategy.LZF;
      if (versionFromBuffer == VERSION) {
        byte compressionId = buffer.get();
        compression = CompressionStrategy.forId(compressionId);
      }
      return CompressionFactory.getDoubleSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          compression,
          fileMapper
      );
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
