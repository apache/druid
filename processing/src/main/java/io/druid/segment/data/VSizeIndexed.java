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
import com.metamx.common.ISE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class VSizeIndexed implements Indexed<VSizeIndexedInts>
{
  private static final byte version = 0x1;

  public static VSizeIndexed fromIterable(Iterable<VSizeIndexedInts> objectsIterable)
  {
    Iterator<VSizeIndexedInts> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      return new VSizeIndexed(buffer, 4);
    }

    int numBytes = -1;

    int count = 0;
    while (objects.hasNext()) {
      VSizeIndexedInts next = objects.next();
      if (numBytes == -1) {
        numBytes = next.getNumBytes();
      }
      ++count;
    }

    ByteArrayOutputStream headerBytes = new ByteArrayOutputStream(4 + (count * 4));
    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    int offset = 0;

    try {
      headerBytes.write(Ints.toByteArray(count));

      for (VSizeIndexedInts object : objectsIterable) {
        if (object.getNumBytes() != numBytes) {
          throw new ISE("val.numBytes[%s] != numBytesInValue[%s]", object.getNumBytes(), numBytes);
        }
        byte[] bytes = object.getBytesNoPadding();
        offset += bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        valueBytes.write(bytes);
      }
      valueBytes.write(new byte[4 - numBytes]);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer theBuffer = ByteBuffer.allocate(headerBytes.size() + valueBytes.size());
    theBuffer.put(headerBytes.toByteArray());
    theBuffer.put(valueBytes.toByteArray());
    theBuffer.flip();

    return new VSizeIndexed(theBuffer.asReadOnlyBuffer(), numBytes);
  }

  private final ByteBuffer theBuffer;
  private final int numBytes;
  private final int size;

  private final int valuesOffset;
  private final int bufferBytes;

  VSizeIndexed(
      ByteBuffer buffer,
      int numBytes
  )
  {
    this.theBuffer = buffer;
    this.numBytes = numBytes;

    size = theBuffer.getInt();
    valuesOffset = theBuffer.position() + (size << 2);
    bufferBytes = 4 - numBytes;
  }

  @Override
  public Class<? extends VSizeIndexedInts> getClazz()
  {
    return VSizeIndexedInts.class;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public VSizeIndexedInts get(int index)
  {
    if (index >= size) {
      throw new IllegalArgumentException(String.format("Index[%s] >= size[%s]", index, size));
    }

    ByteBuffer myBuffer = theBuffer.asReadOnlyBuffer();
    int startOffset = 0;
    int endOffset;

    if (index == 0) {
      endOffset = myBuffer.getInt();
    } else {
      myBuffer.position(myBuffer.position() + ((index - 1) * Ints.BYTES));
      startOffset = myBuffer.getInt();
      endOffset = myBuffer.getInt();
    }

    myBuffer.position(valuesOffset + startOffset);
    myBuffer.limit(myBuffer.position() + (endOffset - startOffset) + bufferBytes);
    return myBuffer.hasRemaining() ? new VSizeIndexedInts(myBuffer, numBytes) : null;
  }

  @Override
  public int indexOf(VSizeIndexedInts value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  public int getSerializedSize()
  {
    return theBuffer.remaining() + 4 + 4 + 2;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }

  public static VSizeIndexed readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (version == versionFromBuffer) {
      int numBytes = buffer.get();
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new VSizeIndexed(bufferToUse, numBytes);
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @Override
  public Iterator<VSizeIndexedInts> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }
}
