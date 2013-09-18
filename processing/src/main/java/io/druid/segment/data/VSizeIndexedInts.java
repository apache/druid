/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.metamx.common.IAE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

/**
 */
public class VSizeIndexedInts implements IndexedInts, Comparable<VSizeIndexedInts>
{
  private static final byte version = 0x0;

  public static VSizeIndexedInts fromArray(int[] array)
  {
    return fromArray(array, Ints.max(array));
  }

  public static VSizeIndexedInts fromArray(int[] array, int maxValue)
  {
    return fromList(Ints.asList(array), maxValue);
  }

  public static VSizeIndexedInts fromList(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes) + (4 - numBytes));
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

    return new VSizeIndexedInts(buffer.asReadOnlyBuffer(), numBytes);
  }

  public static byte getNumBytesForMax(int maxValue)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }

    byte numBytes = 4;
    if (maxValue <= 0xFF) {
      numBytes = 1;
    }
    else if (maxValue <= 0xFFFF) {
      numBytes = 2;
    }
    else if (maxValue <= 0xFFFFFF) {
      numBytes = 3;
    }
    return numBytes;
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

  public byte[] getBytesNoPadding()
  {
    int bytesToTake = buffer.remaining() - (4 - numBytes);
    byte[] bytes = new byte[bytesToTake];
    buffer.asReadOnlyBuffer().get(bytes);
    return bytes;
  }

  public byte[] getBytes()
  {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.asReadOnlyBuffer().get(bytes);
    return bytes;
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

  public int getSerializedSize()
  {
    return 1 + 1 + 4 + buffer.remaining();
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new IndexedIntsIterator(this);
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(buffer.remaining())));
    channel.write(buffer.asReadOnlyBuffer());
  }

  public static VSizeIndexedInts readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (version == versionFromBuffer) {
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

}
