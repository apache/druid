/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.segment.data;

import com.google.common.primitives.Ints;
import org.apache.druid.common.utils.ByteUtils;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class VSizeColumnarMultiInts implements ColumnarMultiInts, WritableSupplier<ColumnarMultiInts>
{
  private static final byte VERSION = 0x1;

  private static final MetaSerdeHelper<VSizeColumnarMultiInts> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((VSizeColumnarMultiInts x) -> VERSION)
      .writeByte(x -> ByteUtils.checkedCast(x.numBytes))
      .writeInt(x -> Ints.checkedCast(x.theBuffer.remaining() + (long) Integer.BYTES))
      .writeInt(x -> x.size);

  public static VSizeColumnarMultiInts fromIterable(Iterable<VSizeColumnarInts> objectsIterable)
  {
    Iterator<VSizeColumnarInts> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(0, 0);
      return new VSizeColumnarMultiInts(buffer, Integer.BYTES);
    }

    int numBytes = -1;

    int count = 0;
    while (objects.hasNext()) {
      VSizeColumnarInts next = objects.next();
      if (numBytes == -1) {
        numBytes = next.getNumBytes();
      }
      ++count;
    }

    HeapByteBufferWriteOutBytes headerBytes = new HeapByteBufferWriteOutBytes();
    HeapByteBufferWriteOutBytes valueBytes = new HeapByteBufferWriteOutBytes();
    int offset = 0;
    headerBytes.writeInt(count);

    for (VSizeColumnarInts object : objectsIterable) {
      if (object.getNumBytes() != numBytes) {
        throw new ISE("val.numBytes[%s] != numBytesInValue[%s]", object.getNumBytes(), numBytes);
      }
      offset += object.getNumBytesNoPadding();
      headerBytes.writeInt(offset);
      object.writeBytesNoPaddingTo(valueBytes);
    }
    valueBytes.write(new byte[Integer.BYTES - numBytes]);

    ByteBuffer theBuffer = ByteBuffer.allocate(Ints.checkedCast(headerBytes.size() + valueBytes.size()));
    headerBytes.writeTo(theBuffer);
    valueBytes.writeTo(theBuffer);
    theBuffer.flip();

    return new VSizeColumnarMultiInts(theBuffer.asReadOnlyBuffer(), numBytes);
  }

  private final ByteBuffer theBuffer;
  private final int numBytes;
  private final int size;

  private final int valuesOffset;
  private final int bufferBytes;

  VSizeColumnarMultiInts(
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
  public int size()
  {
    return size;
  }

  @Override
  public VSizeColumnarInts get(int index)
  {
    if (index >= size) {
      throw new IAE("Index[%d] >= size[%d]", index, size);
    }

    ByteBuffer myBuffer = theBuffer.asReadOnlyBuffer();
    int startOffset = 0;
    int endOffset;

    if (index == 0) {
      endOffset = myBuffer.getInt();
    } else {
      myBuffer.position(myBuffer.position() + ((index - 1) * Integer.BYTES));
      startOffset = myBuffer.getInt();
      endOffset = myBuffer.getInt();
    }

    myBuffer.position(valuesOffset + startOffset);
    myBuffer.limit(myBuffer.position() + (endOffset - startOffset) + bufferBytes);
    return myBuffer.hasRemaining() ? new VSizeColumnarInts(myBuffer, numBytes) : null;
  }

  @Override
  public IndexedInts getUnshared(final int index)
  {
    return get(index);
  }

  @Override
  public int indexOf(IndexedInts value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  @Override
  public long getSerializedSize()
  {
    return META_SERDE_HELPER.size(this) + (long) theBuffer.remaining();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    Channels.writeFully(channel, theBuffer.asReadOnlyBuffer());
  }

  @Override
  public ColumnarMultiInts get()
  {
    return this;
  }

  public static VSizeColumnarMultiInts readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION == versionFromBuffer) {
      int numBytes = buffer.get();
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new VSizeColumnarMultiInts(bufferToUse, numBytes);
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @Override
  public Iterator<IndexedInts> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }

  @Override
  public void close()
  {
    // no-op
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("theBuffer", theBuffer);
  }
}
