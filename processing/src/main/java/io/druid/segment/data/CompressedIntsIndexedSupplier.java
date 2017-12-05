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
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
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

public class CompressedIntsIndexedSupplier implements WritableSupplier<IndexedInts>
{
  public static final byte VERSION = 0x2;
  public static final int MAX_INTS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Ints.BYTES;

  private static MetaSerdeHelper<CompressedIntsIndexedSupplier> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedIntsIndexedSupplier x) -> VERSION)
      .writeInt(x -> x.totalSize)
      .writeInt(x -> x.sizePer)
      .writeByte(x -> x.compression.getId());

  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseIntBuffers;
  private final CompressionStrategy compression;

  private CompressedIntsIndexedSupplier(
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
  public IndexedInts get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if (powerOf2) {
      return new CompressedIndexedInts()
      {
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
      return new CompressedIndexedInts();
    }
  }

  @Override
  public long getSerializedSize() throws IOException
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
  GenericIndexed<?> getBaseIntBuffers()
  {
    return baseIntBuffers;
  }

  public static CompressedIntsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressionStrategy compression = CompressionStrategy.forId(buffer.get());
      return new CompressedIntsIndexedSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(buffer, new DecompressingByteBufferObjectStrategy(order, compression)),
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @VisibleForTesting
  static CompressedIntsIndexedSupplier fromIntBuffer(
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

    return new CompressedIntsIndexedSupplier(
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
                      .allocateInBuffer(chunkFactor * Ints.BYTES, closer)
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
                    retVal.clear().limit(retValAsIntBuffer.position() * Ints.BYTES);
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
            chunkFactor * Ints.BYTES,
            byteOrder,
            closer
        ),
        compression
    );
  }

  @VisibleForTesting
  public static CompressedIntsIndexedSupplier fromList(
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

    return new CompressedIntsIndexedSupplier(
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
                      .allocateInBuffer(chunkFactor * Ints.BYTES, closer)
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
            chunkFactor * Ints.BYTES,
            byteOrder,
            closer
        ),
        compression
    );
  }

  private class CompressedIndexedInts implements IndexedInts
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedIntBuffers = baseIntBuffers.singleThreaded();

    int currIndex = -1;
    ResourceHolder<ByteBuffer> holder;
    IntBuffer buffer;

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

      return buffer.get(buffer.position() + bufferIndex);
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedIntBuffers.get(bufferNum);
      buffer = holder.get().asIntBuffer();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedIntsIndexedSupplier_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedIntBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
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
