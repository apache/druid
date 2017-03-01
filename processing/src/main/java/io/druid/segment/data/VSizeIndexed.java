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
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.PositionalMemoryRegion;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class VSizeIndexed implements IndexedMultivalue<IndexedInts>
{
  private static final byte version = 0x1;

  public static VSizeIndexed fromIterable(Iterable<VSizeIndexedInts> objectsIterable)
  {
    Iterator<VSizeIndexedInts> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      return new VSizeIndexed(new NativeMemory(buffer), 4);
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

    return new VSizeIndexed(new NativeMemory(theBuffer), numBytes);
  }

  private final Memory memory;
  private final int numBytes;
  private final int size;

  private final int valuesOffset;
  private final int bufferBytes;

  VSizeIndexed(
      Memory memory,
      int numBytes
  )
  {
    this.memory = memory;
    this.numBytes = numBytes;

    size = Integer.reverseBytes(this.memory.getInt(0));
    valuesOffset = Integer.BYTES + (size << 2);
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

//    ByteBuffer myBuffer = theBuffer.asReadOnlyBuffer();
    int startOffset = 0;
    int endOffset;

    if (index == 0) {
      endOffset = Integer.reverseBytes(memory.getInt(Integer.BYTES));
    } else {
      startOffset = Integer.reverseBytes(memory.getInt((index ) * Ints.BYTES));
      endOffset = Integer.reverseBytes(memory.getInt((index + 1) * Ints.BYTES));
    }

    int position = valuesOffset + startOffset;
    int limit = position + (endOffset - startOffset) + bufferBytes;
    return memory.getCapacity() > position ? new VSizeIndexedInts(new MemoryRegion(memory, position, limit - position), numBytes) : null;
  }

  @Override
  public int indexOf(IndexedInts value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  public int getSerializedSize()
  {
    return (int)memory.getCapacity() + 4 + 2;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray((int)memory.getCapacity())));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));

    byte[] bytes = new byte[(int)memory.getCapacity()-4];
    memory.getByteArray(4, bytes, 0, bytes.length);
    channel.write(ByteBuffer.wrap(bytes));
  }

  public static VSizeIndexed readFromMemory(PositionalMemoryRegion fromMemory)
  {
    byte versionFromBuffer = fromMemory.getByte();

    if (version == versionFromBuffer) {
      int numBytes = fromMemory.getByte();
      int size = Integer.reverseBytes(fromMemory.getInt());
      PositionalMemoryRegion memoryToUse = fromMemory.duplicate();
      memoryToUse.limit(memoryToUse.position() + size);
      fromMemory.position(memoryToUse.limit());

      return new VSizeIndexed(memoryToUse.getRemainingMemory(), numBytes);
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @Override
  public Iterator<IndexedInts> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }

  @Override
  public void close() throws IOException
  {
    // no-op
  }

  public WritableSupplier<IndexedMultivalue<IndexedInts>> asWritableSupplier() {
    return new VSizeIndexedSupplier(this);
  }

  public static class VSizeIndexedSupplier implements WritableSupplier<IndexedMultivalue<IndexedInts>> {
    final VSizeIndexed delegate;

    public VSizeIndexedSupplier(VSizeIndexed delegate) {
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
    public IndexedMultivalue<IndexedInts> get()
    {
      return delegate;
    }
  }
}
