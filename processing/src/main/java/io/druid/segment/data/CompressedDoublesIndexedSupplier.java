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

package io.druid.segment.data;

import com.google.common.base.Supplier;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedDoublesIndexedSupplier
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;

  private CompressedDoublesIndexedSupplier()
  {
  }

  public static Supplier<IndexedDoubles> fromByteBuffer(
      ByteBuffer buffer,
      ByteOrder order,
      SmooshedFileMapper mapper
  )
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == version) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
      if (versionFromBuffer == version) {
        byte compressionId = buffer.get();
        compression = CompressedObjectStrategy.CompressionStrategy.forId(compressionId);
      }
      return CompressionFactory.getDoubleSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          compression,
          mapper
      );
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

}
