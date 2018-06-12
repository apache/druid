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
import io.druid.common.utils.ByteUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.CompressedPools;
import io.druid.segment.serde.MetaSerdeHelper;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

public class CompressedVSizeColumnarIntsSupplier implements WritableSupplier<ColumnarInts>
{
  public static final byte VERSION = 0x2;

  private static final MetaSerdeHelper<CompressedVSizeColumnarIntsSupplier> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedVSizeColumnarIntsSupplier x) -> VERSION)
      .writeByte(x -> ByteUtils.checkedCast(x.numBytes))
      .writeInt(x -> x.totalSize)
      .writeInt(x -> x.sizePer)
      .writeByte(x -> x.compression.getId());

  private final int totalSize;
  private final int sizePer;
  private final int numBytes;
  private final int bigEndianShift;
  private final int littleEndianMask;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers;
  private final CompressionStrategy compression;

  private CompressedVSizeColumnarIntsSupplier(
      int totalSize,
      int sizePer,
      int numBytes,
      GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers,
      CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        sizePer == (1 << Integer.numberOfTrailingZeros(sizePer)),
        "Number of entries per chunk must be a power of 2"
    );

    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseBuffers = baseBuffers;
    this.compression = compression;
    this.numBytes = numBytes;
    this.bigEndianShift = Integer.SIZE - (numBytes << 3); // numBytes * 8
    this.littleEndianMask = (int) ((1L << (numBytes << 3)) - 1); // set numBytes * 8 lower bits to 1
  }

  public static int maxIntsInBufferForBytes(int numBytes)
  {
    int maxSizePer = (CompressedPools.BUFFER_SIZE - bufferPadding(numBytes)) / numBytes;
    // round down to the nearest power of 2
    return Integer.highestOneBit(maxSizePer);
  }

  static int bufferPadding(int numBytes)
  {
    // when numBytes == 3 we need to pad the buffer to allow reading an extra byte
    // beyond the end of the last value, since we use buffer.getInt() to read values.
    // for numBytes 1, 2 we remove the need for padding by reading bytes or shorts directly.
    switch (numBytes) {
      case Short.BYTES:
      case 1:
        return 0;
      default:
        return Integer.BYTES - numBytes;
    }
  }

  public static int maxIntsInBufferForValue(int maxValue)
  {
    return maxIntsInBufferForBytes(VSizeColumnarInts.getNumBytesForMax(maxValue));
  }

  @Override
  public ColumnarInts get()
  {
    // optimized versions for int, short, and byte columns
    if (numBytes == Integer.BYTES) {
      return new CompressedFullSizeColumnarInts();
    } else if (numBytes == Short.BYTES) {
      return new CompressedShortSizeColumnarInts();
    } else if (numBytes == 1) {
      return new CompressedByteSizeColumnarInts();
    } else {
      // default version of everything else, i.e. 3-bytes per value
      return new CompressedVSizeColumnarInts();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return metaSerdeHelper.size(this) + baseBuffers.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    metaSerdeHelper.writeTo(channel, this);
    baseBuffers.writeTo(channel, smoosher);
  }

  @VisibleForTesting
  GenericIndexed<ResourceHolder<ByteBuffer>> getBaseBuffers()
  {
    return baseBuffers;
  }

  public static CompressedVSizeColumnarIntsSupplier fromByteBuffer(
      ByteBuffer buffer,
      ByteOrder order
  )
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      final int numBytes = buffer.get();
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();

      final CompressionStrategy compression = CompressionStrategy.forId(buffer.get());

      return new CompressedVSizeColumnarIntsSupplier(
          totalSize,
          sizePer,
          numBytes,
          GenericIndexed.read(buffer, new DecompressingByteBufferObjectStrategy(order, compression)),
          compression
      );

    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @VisibleForTesting
  public static CompressedVSizeColumnarIntsSupplier fromList(
      final IntList list,
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final Closer closer
  )
  {
    final int numBytes = VSizeColumnarInts.getNumBytesForMax(maxValue);
    final int chunkBytes = chunkFactor * numBytes;

    Preconditions.checkArgument(
        chunkFactor <= maxIntsInBufferForBytes(numBytes),
        "Chunks must be <= 64k bytes. chunkFactor was[%s]",
        chunkFactor
    );

    return new CompressedVSizeColumnarIntsSupplier(
        list.size(),
        chunkFactor,
        numBytes,
        GenericIndexed.ofCompressedByteBuffers(
            new Iterable<ByteBuffer>()
            {
              @Override
              public Iterator<ByteBuffer> iterator()
              {
                return new Iterator<ByteBuffer>()
                {
                  int position = 0;
                  private final ByteBuffer retVal =
                      compression.getCompressor().allocateInBuffer(chunkBytes, closer).order(byteOrder);
                  private final boolean isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
                  private final ByteBuffer helperBuf = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ByteBuffer next()
                  {
                    retVal.clear();
                    int elementCount = Math.min(list.size() - position, chunkFactor);
                    retVal.limit(numBytes * elementCount);

                    for (int limit = position + elementCount; position < limit; position++) {
                      writeIntToRetVal(list.getInt(position));
                    }
                    retVal.rewind();
                    return retVal;
                  }

                  private void writeIntToRetVal(int value)
                  {
                    helperBuf.putInt(0, value);
                    if (isBigEndian) {
                      retVal.put(helperBuf.array(), Integer.BYTES - numBytes, numBytes);
                    } else {
                      retVal.put(helperBuf.array(), 0, numBytes);
                    }
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
            chunkBytes,
            byteOrder,
            closer
        ),
        compression
    );
  }

  private class CompressedFullSizeColumnarInts extends CompressedVSizeColumnarInts
  {
    @Override
    protected int _get(ByteBuffer buffer, boolean bigEngian, int bufferIndex)
    {
      return buffer.getInt(bufferIndex * Integer.BYTES);
    }
  }

  private class CompressedShortSizeColumnarInts extends CompressedVSizeColumnarInts
  {
    @Override
    protected int _get(ByteBuffer buffer, boolean bigEngian, int bufferIndex)
    {
      // removes the need for padding
      return buffer.getShort(bufferIndex * Short.BYTES) & 0xFFFF;
    }
  }

  private class CompressedByteSizeColumnarInts extends CompressedVSizeColumnarInts
  {
    @Override
    protected int _get(ByteBuffer buffer, boolean bigEngian, int bufferIndex)
    {
      // removes the need for padding
      return buffer.get(bufferIndex) & 0xFF;
    }
  }

  private class CompressedVSizeColumnarInts implements ColumnarInts
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedBuffers = baseBuffers.singleThreaded();

    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;

    int currBufferNum = -1;
    ResourceHolder<ByteBuffer> holder;
    /** buffer's position must be 0 */
    ByteBuffer buffer;
    boolean bigEndian;

    @Override
    public int size()
    {
      return totalSize;
    }

    /**
     * Returns the value at the given index into the column.
     * <p/>
     * Assumes the number of entries in each decompression buffers is a power of two.
     *
     * @param index index of the value in the column
     *
     * @return the value at the given index
     */
    @Override
    public int get(int index)
    {
      // assumes the number of entries in each buffer is a power of 2
      final int bufferNum = index >> div;
      final int bufferIndex = index & rem;

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return _get(buffer, bigEndian, bufferIndex);
    }

    /**
     * Returns the value at the given bufferIndex in the current decompression buffer
     *
     * @param bufferIndex index of the value in the current buffer
     *
     * @return the value at the given bufferIndex
     */
    int _get(ByteBuffer buffer, boolean bigEndian, final int bufferIndex)
    {
      final int pos = bufferIndex * numBytes;
      // example for numBytes = 3
      // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
      // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
      return bigEndian ?
             buffer.getInt(pos) >>> bigEndianShift :
             buffer.getInt(pos) & littleEndianMask;
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedBuffers.get(bufferNum);
      ByteBuffer bb = holder.get();
      ByteOrder byteOrder = bb.order();
      // slice() makes the buffer's position = 0
      buffer = bb.slice().order(byteOrder);
      currBufferNum = bufferNum;
      bigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
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
      return "CompressedVSizeColumnarInts{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // ideally should inspect buffer and bigEndian, but at the moment of inspectRuntimeShape() call buffer is likely
      // to be null and bigEndian = false, because loadBuffer() is not yet called, although during the processing buffer
      // is not null, hence "visiting" null is not representative, and visiting bigEndian = false could be misleading.
      inspector.visit("singleThreadedBuffers", singleThreadedBuffers);
    }
  }
}
