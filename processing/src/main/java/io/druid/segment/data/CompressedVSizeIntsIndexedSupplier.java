/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

public class CompressedVSizeIntsIndexedSupplier implements Supplier<IndexedInts>
{
  public static final byte version = 0x2;

  private final int totalSize;
  private final int sizePer;
  private final int numBytes;
  private final int bitsToShift;
  private final int bitMask;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  public CompressedVSizeIntsIndexedSupplier(
      int totalSize,
      int sizePer,
      int numBytes,
      GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseBuffers = baseBuffers;
    this.compression = compression;
    this.numBytes = numBytes;
    this.bitsToShift = Integer.SIZE - (numBytes << 3); // numBytes * 8
    this.bitMask = (1 << (numBytes << 3)) - 1;
  }

  public static int maxIntsInBufferForBytes(int numBytes) {
    return CompressedPools.BUFFER_SIZE / numBytes;
  }

  public static int maxIntsInBufferForValue(int maxValue) {
    return maxIntsInBufferForBytes(VSizeIndexedInts.getNumBytesForMax(maxValue));
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedInts get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if(powerOf2) {
      return new CompressedVSizeIndexedInts() {
        @Override
        public int get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currIndex) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return buffer.get(buffer.position() + bufferIndex);
        }
      };
    } else {
      return new CompressedVSizeIndexedInts();
    }
  }


  public long getSerializedSize()
  {
    return 1 +             // version
           1 +             // numBytes
           Integer.BYTES + // totalSize
           Integer.BYTES + // sizePer
           1 +             // compression id
           baseBuffers.getSerializedSize(); // data
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseBuffers.writeToChannel(channel);
  }

  public CompressedVSizeIntsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedVSizeIntsIndexedSupplier(
        totalSize,
        sizePer,
        numBytes,
        GenericIndexed.fromIterable(baseBuffers, CompressedByteBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
        compression
    );
  }

  /**
   * For testing.  Do not use unless you like things breaking
   */
  GenericIndexed<ResourceHolder<ByteBuffer>> getBaseBuffers()
  {
    return baseBuffers;
  }

  public static CompressedVSizeIntsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version) {
      final int numBytes = buffer.get();
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.forId(buffer.get());
      return new CompressedVSizeIntsIndexedSupplier(
          totalSize,
          sizePer,
          numBytes,
          GenericIndexed.read(buffer, CompressedByteBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

//  public static CompressedVSizeIntsIndexedSupplier fromIntBuffer(IntBuffer buffer, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression)
//  {
//    return fromIntBuffer(buffer, MAX_INTS_IN_BUFFER, byteOrder, compression);
//  }
//
//  public static CompressedVSizeIntsIndexedSupplier fromIntBuffer(
//      final IntBuffer buffer, final int chunkFactor, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression
//  )
//  {
//    Preconditions.checkArgument(
//        chunkFactor <= MAX_INTS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
//    );
//
//    return new CompressedVSizeIntsIndexedSupplier(
//        buffer.remaining(),
//        chunkFactor,
//        numBytes,
//        GenericIndexed.fromIterable(
//            new Iterable<ResourceHolder<IntBuffer>>()
//            {
//              @Override
//              public Iterator<ResourceHolder<IntBuffer>> iterator()
//              {
//                return new Iterator<ResourceHolder<ByteBuffer>>()
//                {
//                  ByteBuffer myBuffer = buffer.asReadOnlyBuffer();
//
//                  @Override
//                  public boolean hasNext()
//                  {
//                    return myBuffer.hasRemaining();
//                  }
//
//                  @Override
//                  public ResourceHolder<IntBuffer> next()
//                  {
//                    IntBuffer retVal = myBuffer.asReadOnlyBuffer();
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
//            CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
//        ),
//        compression
//    );
//  }

  public static CompressedVSizeIntsIndexedSupplier fromList(
      final List<Integer> list, final int maxValue, final int chunkFactor, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    final int numBytes = VSizeIndexedInts.getNumBytesForMax(maxValue);

    Preconditions.checkArgument(
        chunkFactor <= maxIntsInBufferForBytes(numBytes), "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedVSizeIntsIndexedSupplier(
        list.size(),
        chunkFactor,
        numBytes,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<ByteBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<ByteBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<ByteBuffer>>()
                {
                  // always leave extra bytes to read a full int
                  final int chunkBytes = chunkFactor * numBytes + (Ints.BYTES - numBytes);
                  int position = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ResourceHolder<ByteBuffer> next()
                  {
                    ByteBuffer retVal = ByteBuffer
                        .allocate(chunkBytes)
                        .order(byteOrder);

                    if (chunkFactor > list.size() - position) {
                      retVal.limit((list.size() - position) * numBytes);
                    }

                    final List<Integer> ints = list.subList(position, position + retVal.remaining());
                    for(int value : ints) {
                      if(byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
                        byte[] intAsBytes = Ints.toByteArray(value);
                        retVal.put(intAsBytes, intAsBytes.length - numBytes, numBytes);
                      } else {
                        final ByteBuffer buf = ByteBuffer
                            .allocate(Ints.BYTES)
                            .order(byteOrder)
                            .putInt(value);
                        // TODO check if this byte[] thing is faster or not
//                        byte[] bytes = new byte[]{
//                            (byte) value,
//                            (byte) (value << 8),
//                            (byte) (value << 16)
//                        };
                        retVal.put(buf.array(), 0, numBytes);
                      }
                    }
                    retVal.rewind();
                    position += retVal.remaining() / numBytes;

                    return StupidResourceHolder.create(retVal);
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException();
                  }
                };
              }
            },
            CompressedByteBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
        ),
        compression
    );
  }

  private class CompressedVSizeIndexedInts implements IndexedInts
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedBuffers = baseBuffers.singleThreaded();

    int currIndex = -1;
    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public int get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      // big-endian: 0x000c0b0a stored  0c 0b 0a XX, read 0x0c0b0aXX >>> 8
      // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
      return buffer.order().equals(ByteOrder.BIG_ENDIAN) ?
             buffer.getInt(buffer.position() + bufferIndex) >>> bitsToShift :
             buffer.getInt(buffer.position() + bufferIndex) & bitMask;
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return new IndexedIntsIterator(this);
    }

    @Override
    public void fill(int index, int[] toFill)
    {
      throw new UnsupportedOperationException("fill not supported");
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedBuffers.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedVSizedIntsIndexedSupplier{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
    }
  }
}
