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
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedDoublesIndexedSupplier implements Supplier<IndexedDoubles>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;

  private final int totalSize;
  private final int sizePer;
  private final ByteBuffer buffer;
  private final Supplier<IndexedDoubles> supplier;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  public CompressedDoublesIndexedSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer buffer,
      Supplier<IndexedDoubles> supplier,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = buffer;
    this.supplier = supplier;
    this.compression = compression;
  }

  @Override
  public IndexedDoubles get()
  {
    return supplier.get();
  }

  public static CompressedDoublesIndexedSupplier fromByteBuffer(
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
      Supplier<IndexedDoubles> supplier = CompressionFactory.getDoubleSupplier(
          totalSize,
          sizePer,
          buffer.asReadOnlyBuffer(),
          order,
          compression,
          mapper
      );
      return new CompressedDoublesIndexedSupplier(
          totalSize,
          sizePer,
          buffer,
          supplier,
          compression
      );
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public int size()
  {
    return totalSize;
  }

  public long getSerializedSize()
  {
    return buffer.remaining() + 1 + 4 + 4 + 1;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    channel.write(buffer.asReadOnlyBuffer());
  }
}
