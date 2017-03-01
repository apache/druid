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
import io.druid.java.util.common.io.smoosh.PositionalMemoryRegion;

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
  private final Memory memory;
  private final Supplier<IndexedFloats> supplier;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  CompressedFloatsIndexedSupplier(
      int totalSize,
      int sizePer,
      Memory memory,
      Supplier<IndexedFloats> supplier,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.memory = memory;
    this.supplier = supplier;
    this.compression = compression;
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
    return memory.getCapacity() + 1 + 4 + 4 + 1;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
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

  public static CompressedFloatsIndexedSupplier fromMemory(PositionalMemoryRegion pMemory, ByteOrder order)
  {
    byte versionFromBuffer = pMemory.getByte();

    if (versionFromBuffer == LZF_VERSION || versionFromBuffer == version) {
      final int totalSize = Integer.reverseBytes(pMemory.getInt());
      final int sizePer = Integer.reverseBytes(pMemory.getInt());
      CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
      if (versionFromBuffer == version) {
        byte compressionId = pMemory.getByte();
        compression = CompressedObjectStrategy.CompressionStrategy.forId(compressionId);
      }
      Supplier<IndexedFloats> supplier = CompressionFactory.getFloatSupplier(
          totalSize,
          sizePer,
          pMemory.getRemainingMemory(),
          order,
          compression
      );
      return new CompressedFloatsIndexedSupplier(
          totalSize,
          sizePer,
          pMemory.getRemainingMemory(),
          supplier,
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
