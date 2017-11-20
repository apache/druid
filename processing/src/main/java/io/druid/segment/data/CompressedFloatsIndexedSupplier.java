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
import io.druid.io.Channels;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.serde.MetaSerdeHelper;
import io.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedFloatsIndexedSupplier implements Supplier<IndexedFloats>, Serializer
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte VERSION = 0x2;

  private static final MetaSerdeHelper<CompressedFloatsIndexedSupplier> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedFloatsIndexedSupplier x) -> VERSION)
      .writeInt(x -> x.totalSize)
      .writeInt(x -> x.sizePer)
      .writeByte(x -> x.compression.getId());

  private final int totalSize;
  private final int sizePer;
  private final ByteBuffer buffer;
  private final Supplier<IndexedFloats> supplier;
  private final CompressionStrategy compression;

  CompressedFloatsIndexedSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer buffer,
      Supplier<IndexedFloats> supplier,
      CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = buffer;
    this.supplier = supplier;
    this.compression = compression;
  }

  @Override
  public IndexedFloats get()
  {
    return supplier.get();
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return metaSerdeHelper.size(this) + (long) buffer.remaining();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    metaSerdeHelper.writeTo(channel, this);
    Channels.writeFully(channel, buffer.asReadOnlyBuffer());
  }

  public static CompressedFloatsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
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
      Supplier<IndexedFloats> supplier = CompressionFactory.getFloatSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          compression
      );
      return new CompressedFloatsIndexedSupplier(
          totalSize,
          sizePer,
          buffer,
          supplier,
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
