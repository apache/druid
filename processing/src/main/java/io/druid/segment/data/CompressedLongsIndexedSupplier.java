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
import com.metamx.common.IAE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedLongsIndexedSupplier implements Supplier<IndexedLongs>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;


  private final int totalSize;
  private final ByteBuffer buffer;
  private final Supplier<IndexedLongs> supplier;

  CompressedLongsIndexedSupplier(
      int totalSize,
      ByteBuffer buffer,
      Supplier<IndexedLongs> supplier
  )
  {
    this.totalSize = totalSize;
    this.buffer = buffer;
    this.supplier = supplier;
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedLongs get()
  {
    return supplier.get();
  }

  public long getSerializedSize()
  {
    return buffer.remaining();
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(buffer.asReadOnlyBuffer());
  }

  public CompressedLongsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return fromByteBuffer(buffer, order);
  }

  public static CompressedLongsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    byte versionFromBuffer = bufferToUse.get();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == version) {
      final int totalSize = bufferToUse.getInt();
      final int sizePer = bufferToUse.getInt();
      CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
      CompressionFactory.LongEncoding encoding = CompressionFactory.DEFAULT_LONG_ENCODING;
      if (versionFromBuffer == version) {
        byte compressionId = bufferToUse.get();
        if (CompressionFactory.hasFlag(compressionId)) {
          encoding = CompressionFactory.LongEncoding.forId(bufferToUse.get());
          compressionId = CompressionFactory.removeFlag(compressionId);
        }
        compression = CompressedObjectStrategy.CompressionStrategy.forId(compressionId);
      }
      Supplier<IndexedLongs> supplier = CompressionFactory.getLongSupplier(
          totalSize,
          sizePer,
          bufferToUse,
          order,
          encoding,
          compression
      );
      return new CompressedLongsIndexedSupplier(
          totalSize,
          buffer,
          supplier
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
