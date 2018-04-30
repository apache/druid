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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.CompressedPools;
import io.druid.segment.serde.MetaSerdeHelper;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

public class CompressedColumnarIntsSupplier implements WritableSupplier<ColumnarInts>
{
  public static final byte VERSION = 0x2;
  public static final int MAX_INTS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Integer.BYTES;

  private static MetaSerdeHelper<CompressedColumnarIntsSupplier> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedColumnarIntsSupplier x) -> VERSION)
      .writeInt(x -> x.totalSize)
      .writeInt(x -> x.sizePer)
      .writeByte(x -> x.compression.getId());

  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseIntBuffers;
  private final CompressionStrategy compression;

  private CompressedColumnarIntsSupplier(
      int totalSize,
      int sizePer,
      GenericIndexed<ResourceHolder<ByteBuffer>> baseIntBuffers,
      CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseIntBuffers = baseIntBuffers;
    this.compression = compression;
  }

  @Override
  public ColumnarInts get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean isPowerOf2 = sizePer == (1 << div);
    if (isPowerOf2) {
      return new CompressedColumnarInts()
      {
        @Override
        public int get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currBufferNum) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return buffer.get(bufferIndex);
        }
      };
    } else {
      return new CompressedColumnarInts();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return metaSerdeHelper.size(this) + baseIntBuffers.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    metaSerdeHelper.writeTo(channel, this);
    baseIntBuffers.writeTo(channel, smoosher);
  }

  @VisibleForTesting
  GenericIndexed<ResourceHolder<ByteBuffer>> getBaseIntBuffers()
  {
    return baseIntBuffers;
  }

  public static CompressedColumnarIntsSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressionStrategy compression = CompressionStrategy.forId(buffer.get());
      return new CompressedColumnarIntsSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(buffer, new DecompressingByteBufferObjectStrategy(order, compression)),
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @VisibleForTesting
  static CompressedColumnarIntsSupplier fromIntBuffer(
      final IntBuffer buffer,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final Closer closer
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_INTS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedColumnarIntsSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.ofCompressedByteBuffers(
            new Iterable<ByteBuffer>()
            {
              @Override
              public Iterator<ByteBuffer> iterator()
              {
                return new Iterator<ByteBuffer>()
                {
                  final IntBuffer myBuffer = buffer.asReadOnlyBuffer();
                  final ByteBuffer retVal = compression
                      .getCompressor()
                      .allocateInBuffer(chunkFactor * Integer.BYTES, closer)
                      .order(byteOrder);
                  final IntBuffer retValAsIntBuffer = retVal.asIntBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ByteBuffer next()
                  {
                    int initialLimit = myBuffer.limit();
                    if (chunkFactor < myBuffer.remaining()) {
                      myBuffer.limit(myBuffer.position() + chunkFactor);
                    }
                    retValAsIntBuffer.clear();
                    retValAsIntBuffer.put(myBuffer);
                    myBuffer.limit(initialLimit);
                    retVal.clear().limit(retValAsIntBuffer.position() * Integer.BYTES);
                    return retVal;
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException();
                  }
                };
              }
            },
            compression,
            chunkFactor * Integer.BYTES,
            byteOrder,
            closer
        ),
        compression
    );
  }

  @VisibleForTesting
  public static CompressedColumnarIntsSupplier fromList(
      final IntArrayList list,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final Closer closer
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_INTS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedColumnarIntsSupplier(
        list.size(),
        chunkFactor,
        GenericIndexed.ofCompressedByteBuffers(
            new Iterable<ByteBuffer>()
            {
              @Override
              public Iterator<ByteBuffer> iterator()
              {
                return new Iterator<ByteBuffer>()
                {
                  private final ByteBuffer retVal = compression
                      .getCompressor()
                      .allocateInBuffer(chunkFactor * Integer.BYTES, closer)
                      .order(byteOrder);
                  int position = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ByteBuffer next()
                  {
                    int blockSize = Math.min(list.size() - position, chunkFactor);
                    retVal.clear();
                    for (int limit = position + blockSize; position < limit; position++) {
                      retVal.putInt(list.getInt(position));
                    }
                    retVal.flip();
                    return retVal;
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException();
                  }
                };
              }
            },
            compression,
            chunkFactor * Integer.BYTES,
            byteOrder,
            closer
        ),
        compression
    );
  }

  private class CompressedColumnarInts implements ColumnarInts
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedIntBuffers = baseIntBuffers.singleThreaded();

    int currBufferNum = -1;
    ResourceHolder<ByteBuffer> holder;
    /** buffer's position must be 0 */
    IntBuffer buffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public int get(int index)
    {
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return buffer.get(bufferIndex);
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedIntBuffers.get(bufferNum);
      // asIntBuffer() makes the buffer's position = 0
      buffer = holder.get().asIntBuffer();
      currBufferNum = bufferNum;
    }

    @Override
    public void close()
    {
      if (holder != null) {
        holder.close();
      }
    }

    @Override
    public String toString()
    {
      return "CompressedIntsIndexedSupplier_Anonymous{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedIntBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // ideally should inspect buffer, but at the moment of inspectRuntimeShape() call buffer is likely to be null,
      // because loadBuffer() is not yet called, although during the processing it is not null, hence "visiting" null is
      // not representative.
      inspector.visit("singleThreadedIntBuffers", singleThreadedIntBuffers);
    }
  }
}
