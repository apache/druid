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
import com.yahoo.memory.Memory;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.PositionalMemoryRegion;

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
  private final int sizePer;
  private final Memory memory;
  private final Supplier<IndexedLongs> supplier;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final CompressionFactory.LongEncodingFormat encoding;

  CompressedLongsIndexedSupplier(
      int totalSize,
      int sizePer,
      Memory memory,
      Supplier<IndexedLongs> supplier,
      CompressedObjectStrategy.CompressionStrategy compression,
      CompressionFactory.LongEncodingFormat encoding
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.memory = memory;
    this.supplier = supplier;
    this.compression = compression;
    this.encoding = encoding;
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
    return memory.getCapacity() + 1 + 4 + 4 + 1 + (encoding == CompressionFactory.LEGACY_LONG_ENCODING_FORMAT ? 0 : 1);
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    if (encoding == CompressionFactory.LEGACY_LONG_ENCODING_FORMAT) {
      channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    } else {
      channel.write(ByteBuffer.wrap(new byte[]{CompressionFactory.setEncodingFlag(compression.getId())}));
      channel.write(ByteBuffer.wrap(new byte[]{encoding.getId()}));
    }
    long size = memory.getCapacity();
    long bytesOut = 0;
    while(size > bytesOut){
      int bytes = (int) size ;
      if(size > Integer.MAX_VALUE){
        bytes = Integer.MAX_VALUE;
      }
      byte[] byteArray = new byte[bytes];
      memory.getByteArray(bytesOut, byteArray, 0, byteArray.length);
      ByteBuffer bb = ByteBuffer.allocate(bytes).put(byteArray);
      bb.flip();
      channel.write(bb);
      bytesOut += bytes;
    }
  }

  public static CompressedLongsIndexedSupplier fromMemory(PositionalMemoryRegion pMemory, ByteOrder order)
  {
    byte versionFromBuffer = pMemory.getByte();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == version) {
      final int totalSize = Integer.reverseBytes(pMemory.getInt());
      final int sizePer = Integer.reverseBytes(pMemory.getInt());
      CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
      CompressionFactory.LongEncodingFormat encoding = CompressionFactory.LEGACY_LONG_ENCODING_FORMAT;
      if (versionFromBuffer == version) {
        byte compressionId = pMemory.getByte();
        if (CompressionFactory.hasEncodingFlag(compressionId)) {
          encoding = CompressionFactory.LongEncodingFormat.forId(pMemory.getByte());
          compressionId = CompressionFactory.clearEncodingFlag(compressionId);
        }
        compression = CompressedObjectStrategy.CompressionStrategy.forId(compressionId);
      }
      Supplier<IndexedLongs> supplier = CompressionFactory.getLongSupplier(
          totalSize,
          sizePer,
          pMemory.duplicate(),
          order,
          encoding,
          compression
      );
      return new CompressedLongsIndexedSupplier(
          totalSize,
          sizePer,
          pMemory.getRemainingMemory(),
          supplier,
          compression,
          encoding
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

//  public static CompressedLongsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
//  {
//    byte versionFromBuffer = buffer.get();
//
//    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == version) {
//      final int totalSize = buffer.getInt();
//      final int sizePer = buffer.getInt();
//      CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
//      CompressionFactory.LongEncodingFormat encoding = CompressionFactory.LEGACY_LONG_ENCODING_FORMAT;
//      if (versionFromBuffer == version) {
//        byte compressionId = buffer.get();
//        if (CompressionFactory.hasEncodingFlag(compressionId)) {
//          encoding = CompressionFactory.LongEncodingFormat.forId(buffer.get());
//          compressionId = CompressionFactory.clearEncodingFlag(compressionId);
//        }
//        compression = CompressedObjectStrategy.CompressionStrategy.forId(compressionId);
//      }
//      Supplier<IndexedLongs> supplier = CompressionFactory.getLongSupplier(
//          totalSize,
//          sizePer,
//          buffer.asReadOnlyBuffer(),
//          order,
//          encoding,
//          compression
//      );
//      return new CompressedLongsIndexedSupplier(
//          totalSize,
//          sizePer,
//          buffer,
//          supplier,
//          compression,
//          encoding
//      );
//    }
//
//    throw new IAE("Unknown version[%s]", versionFromBuffer);
//  }
}
