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
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ColumnPartSize;
import org.apache.druid.segment.column.ColumnPartSupplier;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedColumnarLongsSupplier implements ColumnPartSupplier<ColumnarLongs>, Serializer
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte VERSION = 0x2;

  private static final MetaSerdeHelper<CompressedColumnarLongsSupplier> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((CompressedColumnarLongsSupplier x) -> VERSION)
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
  private final Supplier<ColumnarLongs> supplier;
  private final CompressionStrategy compression;
  private final CompressionFactory.LongEncodingFormat encoding;
  private final int sizeBytes;

  CompressedColumnarLongsSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer buffer,
      Supplier<ColumnarLongs> supplier,
      CompressionStrategy compression,
      CompressionFactory.LongEncodingFormat encoding,
      int sizeBytes
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = buffer;
    this.supplier = supplier;
    this.compression = compression;
    this.encoding = encoding;
    this.sizeBytes = sizeBytes;
  }

  @Override
  public ColumnarLongs get()
  {
    return supplier.get();
  }

  @Override
  public long getSerializedSize()
  {
    return META_SERDE_HELPER.size(this) + (long) buffer.remaining();
  }

  @Override
  public ColumnPartSize getColumnPartSize()
  {
    return ColumnPartSize.simple(
        StringUtils.format(
            "compressed longs column compression:[%s] values per block:[%s]",
            compression.toString(),
            sizePer
        ),
        sizeBytes
    );
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    Channels.writeFully(channel, buffer.asReadOnlyBuffer());
  }

  public static CompressedColumnarLongsSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    final int startPosition = buffer.position();
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
      Supplier<ColumnarLongs> supplier = CompressionFactory.getLongSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          encoding,
          compression
      );
      final int sizeBytes = buffer.position() - startPosition;
      return new CompressedColumnarLongsSupplier(
          totalSize,
          sizePer,
          buffer,
          supplier,
          compression,
          encoding,
          sizeBytes
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
