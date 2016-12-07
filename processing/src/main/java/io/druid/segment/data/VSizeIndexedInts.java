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

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.IOE;
import io.druid.segment.store.ByteBufferIndexInput;
import io.druid.segment.store.IndexInput;
import io.druid.segment.store.IndexInputUtils;
import io.druid.segment.store.RandomAccessInput;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 */
public class VSizeIndexedInts implements IndexedInts, Comparable<VSizeIndexedInts>
{
  public static final byte VERSION = 0x0;

  public static VSizeIndexedInts fromArray(int[] array)
  {
    return fromArray(array, Ints.max(array));
  }

  public static VSizeIndexedInts fromArray(int[] array, int maxValue)
  {
    return fromList(Ints.asList(array), maxValue);
  }

  public static VSizeIndexedInts empty()
  {
    return fromList(Lists.<Integer>newArrayList(), 0);
  }


  public static VSizeIndexedInts fromArrayIIV(int[] array)
  {
    return fromArrayIIV(array, Ints.max(array));
  }

  public static VSizeIndexedInts fromArrayIIV(int[] array, int maxValue)
  {
    return fromListIIV(Ints.asList(array), maxValue);
  }

  public static VSizeIndexedInts emptyIIV()
  {
    return fromListIIV(Lists.<Integer>newArrayList(), 0);
  }

  /**
   * provide for performance reason.
   */
  public static byte[] getBytesNoPaddingfromList(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);

    return buffer.array();
  }

  public static VSizeIndexedInts fromList(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes) + (4 - numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);

    return new VSizeIndexedInts(buffer.asReadOnlyBuffer(), numBytes);
  }

  public static VSizeIndexedInts fromListIIV(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes) + (4 - numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);
    final ByteBufferIndexInput byteBufferIndexInput = new ByteBufferIndexInput(buffer);
    try {
      return new VSizeIndexedInts(byteBufferIndexInput.duplicate(), numBytes);
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  private static void writeToBuffer(ByteBuffer buffer, List<Integer> list, int numBytes, int maxValue)
  {
    int i = 0;
    for (Integer val : list) {
      if (val < 0) {
        throw new IAE("integer values must be positive, got[%d], i[%d]", val, i);
      }
      if (val > maxValue) {
        throw new IAE("val[%d] > maxValue[%d], please don't lie about maxValue.  i[%d]", val, maxValue, i);
      }

      byte[] intAsBytes = Ints.toByteArray(val);
      buffer.put(intAsBytes, intAsBytes.length - numBytes, numBytes);
      ++i;
    }
    buffer.position(0);
  }


  public static byte getNumBytesForMax(int maxValue)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }

    byte numBytes = 4;
    if (maxValue <= 0xFF) {
      numBytes = 1;
    } else if (maxValue <= 0xFFFF) {
      numBytes = 2;
    } else if (maxValue <= 0xFFFFFF) {
      numBytes = 3;
    }
    return numBytes;
  }

  private final ByteBuffer buffer;
  private final int numBytes;

  private final int bitsToShift;
  private final int size;
  private final IndexInput indexInput;
  private final boolean isIIVersion;
  private final RandomAccessInput randomAccessInput;

  public VSizeIndexedInts(ByteBuffer buffer, int numBytes)
  {
    this.buffer = buffer;
    this.numBytes = numBytes;

    bitsToShift = 32 - (numBytes << 3); // numBytes * 8

    int numBufferBytes = 4 - numBytes;
    size = (buffer.remaining() - numBufferBytes) / numBytes;
    this.indexInput = null;
    this.isIIVersion = false;
    this.randomAccessInput = null;
  }

  public VSizeIndexedInts(IndexInput indexInput, int numBytes)
  {
    try {
      this.buffer = null;
      this.indexInput = indexInput;
      this.isIIVersion = true;
      this.numBytes = numBytes;

      bitsToShift = 32 - (numBytes << 3); // numBytes * 8

      int numBufferBytes = 4 - numBytes;
      int remaining = (int) IndexInputUtils.remaining(indexInput);
      size = (remaining - numBufferBytes) / numBytes;
      randomAccessInput = indexInput.randomAccess();
    }
    catch (IOException e) {
      throw new IOE(e);
    }

  }


  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    if (!isIIVersion) {
      return buffer.getInt(buffer.position() + (index * numBytes)) >>> bitsToShift;
    } else {
      try {
        int pos = (int) indexInput.getFilePointer();
        return randomAccessInput.readInt(pos + (index * numBytes)) >>> bitsToShift;
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }

  public byte[] getBytesNoPadding()
  {
    if (!isIIVersion) {
      int bytesToTake = buffer.remaining() - (4 - numBytes);
      byte[] bytes = new byte[bytesToTake];
      buffer.asReadOnlyBuffer().get(bytes);
      return bytes;
    } else {
      try {
        int remaining = (int) IndexInputUtils.remaining(indexInput);
        int bytesToTake = remaining - (4 - numBytes);
        byte[] bytes = new byte[bytesToTake];
        indexInput.duplicate().readBytes(bytes, 0, bytesToTake);
        return bytes;
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }

  public byte[] getBytes()
  {
    if (!isIIVersion) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.asReadOnlyBuffer().get(bytes);
      return bytes;
    } else {
      try {
        int remaining = (int) IndexInputUtils.remaining(indexInput);
        byte[] bytes = new byte[remaining];
        indexInput.duplicate().readBytes(bytes, 0, remaining);
        return bytes;
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }

  @Override
  public int compareTo(VSizeIndexedInts o)
  {
    int retVal = Ints.compare(numBytes, o.numBytes);

    if (retVal == 0) {
      if (!isIIVersion) {
        retVal = buffer.compareTo(o.buffer);
      } else {
        retVal = IndexInputUtils.compare(indexInput, o.indexInput);
      }
    }

    return retVal;
  }

  public int getNumBytes()
  {
    return numBytes;
  }

  public long getSerializedSize()
  {
    // version, numBytes, size, remaining
    if (!isIIVersion) {
      return 1 + 1 + 4 + buffer.remaining();
    } else {
      try {
        int remaining = (int) IndexInputUtils.remaining(indexInput);
        return 1 + 1 + 4 + remaining;
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }

  @Override
  public IntIterator iterator()
  {
    return new IndexedIntsIterator(this);
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION, (byte) numBytes}));
    if (!isIIVersion) {
      channel.write(ByteBuffer.wrap(Ints.toByteArray(buffer.remaining())));
      channel.write(buffer.asReadOnlyBuffer());
    } else {
      int remaining = (int) IndexInputUtils.remaining(indexInput);
      channel.write(ByteBuffer.wrap(Ints.toByteArray(remaining)));
      IndexInputUtils.write2Channel(indexInput.duplicate(), channel);
    }
  }

  public static VSizeIndexedInts readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION == versionFromBuffer) {
      int numBytes = buffer.get();
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new VSizeIndexedInts(
          bufferToUse,
          numBytes
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static VSizeIndexedInts readFromIndexInput(IndexInput indexInput) throws IOException
  {
    byte versionFromBuffer = indexInput.readByte();

    if (VERSION == versionFromBuffer) {
      int numBytes = indexInput.readByte();
      int size = indexInput.readInt();
      //IndexInput iiToUse = indexInput.duplicate();
      long currentPos = indexInput.getFilePointer();
      IndexInput iiToUse = indexInput.slice(currentPos, size);
      long refreshPos = currentPos + size;
      indexInput.seek(refreshPos);

      return new VSizeIndexedInts(
          iiToUse,
          numBytes
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @Override
  public void fill(int index, int[] toFill)
  {
    throw new UnsupportedOperationException("fill not supported");
  }

  @Override
  public void close() throws IOException
  {

  }

  public WritableSupplier<IndexedInts> asWritableSupplier()
  {
    return new VSizeIndexedIntsSupplier(this);
  }

  public static class VSizeIndexedIntsSupplier implements WritableSupplier<IndexedInts>
  {
    final VSizeIndexedInts delegate;

    public VSizeIndexedIntsSupplier(VSizeIndexedInts delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public long getSerializedSize()
    {
      return delegate.getSerializedSize();
    }

    @Override
    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      delegate.writeToChannel(channel);
    }

    @Override
    public IndexedInts get()
    {
      return delegate;
    }
  }
}
