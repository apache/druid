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

import com.google.common.primitives.Ints;
import io.druid.common.utils.ByteUtils;
import io.druid.io.Channels;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.serde.MetaSerdeHelper;
import io.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class VSizeIndexedInts implements IndexedInts, Comparable<VSizeIndexedInts>, WritableSupplier<IndexedInts>
{
  public static final byte VERSION = 0x0;

  private static final MetaSerdeHelper<VSizeIndexedInts> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((VSizeIndexedInts x) -> VERSION)
      .writeByte(x -> ByteUtils.checkedCast(x.numBytes))
      .writeInt(x -> x.buffer.remaining());

  public static VSizeIndexedInts fromArray(int[] array)
  {
    return fromArray(array, Ints.max(array));
  }

  public static VSizeIndexedInts fromArray(int[] array, int maxValue)
  {
    return fromList(IntArrayList.wrap(array), maxValue);
  }

  public static VSizeIndexedInts fromList(IntList list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes) + (4 - numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);

    return new VSizeIndexedInts(buffer.asReadOnlyBuffer(), numBytes);
  }

  private static void writeToBuffer(ByteBuffer buffer, IntList list, int numBytes, int maxValue)
  {
    ByteBuffer helperBuffer = ByteBuffer.allocate(Ints.BYTES);
    for (int i = 0; i < list.size(); i++) {
      int val = list.getInt(i);
      if (val < 0) {
        throw new IAE("integer values must be positive, got[%d], i[%d]", val, i);
      }
      if (val > maxValue) {
        throw new IAE("val[%d] > maxValue[%d], please don't lie about maxValue.  i[%d]", val, maxValue, i);
      }

      helperBuffer.putInt(0, val);
      buffer.put(helperBuffer.array(), Ints.BYTES - numBytes, numBytes);
    }
    buffer.position(0);
  }

  public static byte getNumBytesForMax(int maxValue)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }

    if (maxValue <= 0xFF) {
      return 1;
    } else if (maxValue <= 0xFFFF) {
      return 2;
    } else if (maxValue <= 0xFFFFFF) {
      return 3;
    }
    return 4;
  }

  private final ByteBuffer buffer;
  private final int numBytes;

  private final int bitsToShift;
  private final int size;

  public VSizeIndexedInts(ByteBuffer buffer, int numBytes)
  {
    this.buffer = buffer;
    this.numBytes = numBytes;

    bitsToShift = 32 - (numBytes << 3); // numBytes * 8

    int numBufferBytes = 4 - numBytes;
    size = (buffer.remaining() - numBufferBytes) / numBytes;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    return buffer.getInt(buffer.position() + (index * numBytes)) >>> bitsToShift;
  }

  public int getNumBytesNoPadding()
  {
    return buffer.remaining() - (Ints.BYTES - numBytes);
  }

  public void writeBytesNoPaddingTo(HeapByteBufferWriteOutBytes out)
  {
    ByteBuffer toWrite = buffer.slice();
    toWrite.limit(toWrite.limit() - (Ints.BYTES - numBytes));
    out.write(toWrite);
  }

  @Override
  public int compareTo(VSizeIndexedInts o)
  {
    int retVal = Ints.compare(numBytes, o.numBytes);

    if (retVal == 0) {
      retVal = buffer.compareTo(o.buffer);
    }

    return retVal;
  }

  public int getNumBytes()
  {
    return numBytes;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return metaSerdeHelper.size(this) + buffer.remaining();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    metaSerdeHelper.writeTo(channel, this);
    Channels.writeFully(channel, buffer.asReadOnlyBuffer());
  }

  @Override
  public IndexedInts get()
  {
    return this;
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

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("buffer", buffer);
  }
}
