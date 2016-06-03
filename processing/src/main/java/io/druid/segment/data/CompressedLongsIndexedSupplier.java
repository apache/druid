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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class CompressedLongsIndexedSupplier implements Supplier<IndexedLongs>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;
  public static final int MAX_LONGS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Longs.BYTES;


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
      final CompressionFactory.CompressionFormat compression = versionFromBuffer == LZF_VERSION ?
                                                    CompressionFactory.CompressionFormat.LZF :
                                                    CompressionFactory.CompressionFormat.forId(bufferToUse.get());
      Supplier<IndexedLongs> supplier = compression.getLongs(totalSize, sizePer, bufferToUse, order);
      return new CompressedLongsIndexedSupplier(
          totalSize,
          buffer,
          supplier
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

//  public static CompressedLongsIndexedSupplier fromLongBuffer(
//      final LongBuffer buffer, final int chunkFactor, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression
//  )
//  {
//    Preconditions.checkArgument(
//        chunkFactor <= MAX_LONGS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
//    );
//
//    return new CompressedLongsIndexedSupplier(
//        buffer.remaining(),
//        chunkFactor,
//        GenericIndexed.fromIterable(
//            new Iterable<ResourceHolder<LongBuffer>>()
//            {
//              @Override
//              public Iterator<ResourceHolder<LongBuffer>> iterator()
//              {
//                return new Iterator<ResourceHolder<LongBuffer>>()
//                {
//                  LongBuffer myBuffer = buffer.asReadOnlyBuffer();
//
//                  @Override
//                  public boolean hasNext()
//                  {
//                    return myBuffer.hasRemaining();
//                  }
//
//                  @Override
//                  public ResourceHolder<LongBuffer> next()
//                  {
//                    LongBuffer retVal = myBuffer.asReadOnlyBuffer();
//
//                    if (chunkFactor < myBuffer.remaining()) {
//                      retVal.limit(retVal.position() + chunkFactor);
//                    }
//                    myBuffer.position(myBuffer.position() + retVal.remaining());
//
//                    return StupidResourceHolder.create(retVal);
//                  }
//
//                  @Override
//                  public void remove()
//                  {
//                    throw new UnsupportedOperationException();
//                  }
//                };
//              }
//            },
//            CompressedLongBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
//        ),
//        compression
//    );
//  }
}
