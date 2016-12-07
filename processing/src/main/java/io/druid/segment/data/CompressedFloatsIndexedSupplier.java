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
import io.druid.java.util.common.IOE;
import io.druid.segment.store.IndexInput;
import io.druid.segment.store.IndexInputUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedFloatsIndexedSupplier implements Supplier<IndexedFloats>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;

  private final int totalSize;
  private final int sizePer;
  private final ByteBuffer buffer;
  private final Supplier<IndexedFloats> supplier;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final IndexInput indexInput;
  private final boolean isIIVersion;

  CompressedFloatsIndexedSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer buffer,
      Supplier<IndexedFloats> supplier,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = buffer;
    this.supplier = supplier;
    this.compression = compression;
    this.indexInput = null;
    this.isIIVersion = false;
  }

  CompressedFloatsIndexedSupplier(
      int totalSize,
      int sizePer,
      IndexInput indexInput,
      Supplier<IndexedFloats> supplier,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.buffer = null;
    this.supplier = supplier;
    this.compression = compression;
    this.indexInput = indexInput;
    this.isIIVersion = true;
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedFloats get()
  {
    return supplier.get();
  }

  public long getSerializedSize()
  {
    if (!isIIVersion) {
      return buffer.remaining() + 1 + 4 + 4 + 1;
    } else {
      try {
        int remaining = (int) IndexInputUtils.remaining(indexInput);
        return remaining + 1 + 4 + 4 + 1;
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    if (isIIVersion) {
      writeToChannelFromII(channel);
      return;
    }
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    channel.write(buffer.asReadOnlyBuffer());
  }

  private void writeToChannelFromII(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    IndexInputUtils.write2Channel(indexInput.duplicate(), channel);
  }

  public static CompressedFloatsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
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

  public static CompressedFloatsIndexedSupplier fromIndexInput(IndexInput indexInput, ByteOrder order)
      throws IOException
  {
    byte versionFromBuffer = indexInput.readByte();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == version) {
      final int totalSize = indexInput.readInt();
      final int sizePer = indexInput.readInt();
      CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
      if (versionFromBuffer == version) {
        byte compressionId = indexInput.readByte();
        compression = CompressedObjectStrategy.CompressionStrategy.forId(compressionId);
      }
      Supplier<IndexedFloats> supplier = CompressionFactory.getFloatSupplier(
          totalSize,
          sizePer,
          indexInput.duplicate(),
          order,
          compression
      );
      return new CompressedFloatsIndexedSupplier(
          totalSize,
          sizePer,
          indexInput,
          supplier,
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

}
