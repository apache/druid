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

/**
 */
public class CompressedLongsIndexedSupplier implements Supplier<IndexedLongs>, Serializer
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte VERSION = 0x2;

  private static final MetaSerdeHelper<CompressedLongsIndexedSupplier> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedLongsIndexedSupplier x) -> VERSION)
      .writeInt(x -> x.totalSize)
      .writeInt(x -> x.sizePer)
      .maybeWriteByte(
          x -> x.encoding != CompressionFactory.LEGACY_LONG_ENCODING_FORMAT,
          x -> CompressionFactory.setEncodingFlag(x.compression.getId())
      )
      .writeByte(x -> {
        if (x.encoding != CompressionFactory.LEGACY_LONG_ENCODING_FORMAT) {
          return x.encoding.getId();
        } else {
          return x.compression.getId();
        }
      });

  private final int totalSize;
  private final int sizePer;
  private final ByteBuffer buffer;
  private final Supplier<IndexedLongs> supplier;
  private final CompressionStrategy compression;
  private final CompressionFactory.LongEncodingFormat encoding;

  CompressedLongsIndexedSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer buffer,
      Supplier<IndexedLongs> supplier,
      CompressionStrategy compression,
      CompressionFactory.LongEncodingFormat encoding
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = buffer;
    this.supplier = supplier;
    this.compression = compression;
    this.encoding = encoding;
  }

  @Override
  public IndexedLongs get()
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

  public static CompressedLongsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      CompressionStrategy compression = CompressionStrategy.LZF;
      CompressionFactory.LongEncodingFormat encoding = CompressionFactory.LEGACY_LONG_ENCODING_FORMAT;
      if (versionFromBuffer == VERSION) {
        byte compressionId = buffer.get();
        if (CompressionFactory.hasEncodingFlag(compressionId)) {
          encoding = CompressionFactory.LongEncodingFormat.forId(buffer.get());
          compressionId = CompressionFactory.clearEncodingFlag(compressionId);
        }
        compression = CompressionStrategy.forId(compressionId);
      }
      Supplier<IndexedLongs> supplier = CompressionFactory.getLongSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          encoding,
          compression
      );
      return new CompressedLongsIndexedSupplier(
          totalSize,
          sizePer,
          buffer,
          supplier,
          compression,
          encoding
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
