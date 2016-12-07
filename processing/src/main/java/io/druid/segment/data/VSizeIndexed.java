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
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.segment.store.ByteBufferIndexInput;
import io.druid.segment.store.IndexInput;
import io.druid.segment.store.IndexInputUtils;

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
    Pair<ByteBuffer, Integer> pair = makeBbFromIterable(objectsIterable);
    ByteBuffer theBuffer = pair.lhs;
    Integer numBytes = pair.rhs;

    return new VSizeIndexed(theBuffer.asReadOnlyBuffer(), numBytes);
  }

  /**
   * IndexInput version
   * @param objectsIterable
   * @return
   */
  public static VSizeIndexed fromIterableIIV(Iterable<VSizeIndexedInts> objectsIterable)
  {
    Pair<ByteBuffer, Integer> pair = makeBbFromIterable(objectsIterable);
    ByteBuffer theBuffer = pair.lhs;
    Integer numBytes = pair.rhs;
    try {
      ByteBufferIndexInput byteBufferIndexInput = new ByteBufferIndexInput(theBuffer);
      return new VSizeIndexed(byteBufferIndexInput.duplicate(), numBytes);
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  private static Pair<ByteBuffer, Integer> makeBbFromIterable(Iterable<VSizeIndexedInts> objectsIterable)
  {

    Iterator<VSizeIndexedInts> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      Pair<ByteBuffer, Integer> pair = new Pair<>(buffer, 4);
      return pair;
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
    Pair<ByteBuffer, Integer> pair = new Pair<ByteBuffer, Integer>(theBuffer, numBytes);
    return pair;
  }

  private final ByteBuffer theBuffer;
  private final int numBytes;
  private final int size;

  private final int valuesOffset;
  private final int bufferBytes;
  private final IndexInput indexInput;
  private final boolean isIIVersion;


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
    this.indexInput = null;
    this.isIIVersion = false;
  }

  VSizeIndexed(
      IndexInput indexInput,
      int numBytes
  )
  {
    try {
      this.theBuffer = null;
      this.numBytes = numBytes;
      this.indexInput = indexInput;
      size = this.indexInput.readInt();
      int pos = (int) this.indexInput.getFilePointer();
      valuesOffset = pos + (size << 2);
      bufferBytes = 4 - numBytes;
      this.isIIVersion = true;
    }
    catch (IOException e) {
      throw new IOE(e);
    }
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
    if (isIIVersion) {
      return getIIV(index);
    }
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

  /**
   * IndexInput version
   *
   * @param index
   *
   * @return
   */
  private VSizeIndexedInts getIIV(int index)
  {
    if (index >= size) {
      throw new IllegalArgumentException(String.format("Index[%s] >= size[%s]", index, size));
    }
    try {
      IndexInput indexInputToUse = this.indexInput.duplicate();

      int startOffset = 0;
      int endOffset;

      if (index == 0) {
        endOffset = indexInputToUse.readInt();
      } else {
        int currentPos = (int) indexInputToUse.getFilePointer();
        indexInputToUse.seek(currentPos + ((index - 1) * Ints.BYTES));
        startOffset = indexInputToUse.readInt();
        endOffset = indexInputToUse.readInt();
      }



      long refreshPos = valuesOffset + startOffset;
      //indexInputToUse.seek(refreshPos);
      long size = (endOffset - startOffset) + bufferBytes;
      long limit = refreshPos +size;
      boolean hasRemaining = refreshPos < limit;

      if(hasRemaining){
        indexInputToUse = indexInputToUse.slice(refreshPos,size);
      }
      return hasRemaining ? new VSizeIndexedInts(indexInputToUse, numBytes) : null;
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  @Override
  public int indexOf(IndexedInts value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  public int getSerializedSize()
  {
    if (!isIIVersion) {
      return theBuffer.remaining() + 4 + 4 + 2;
    } else {
      try {
        return (int) IndexInputUtils.remaining(indexInput) + 4 + 4 + 2;
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    if (isIIVersion) {
      writeToChannelFromIndexInput(channel);
    } else {
      channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
      channel.write(theBuffer.asReadOnlyBuffer());
    }
  }

  private void writeToChannelFromIndexInput(WritableByteChannel channel)
  {
    try {
      channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
      int remaining = (int) IndexInputUtils.remaining(this.indexInput);
      channel.write(ByteBuffer.wrap(Ints.toByteArray(remaining + 4)));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
      IndexInputUtils.write2Channel(this.indexInput.duplicate(), channel);
    }
    catch (IOException e) {
      throw new IOE(e);
    }
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

  public static VSizeIndexed readFromIndexInput(IndexInput indexInput) throws IOException
  {
    byte versionFromBuffer = indexInput.readByte();

    if (version == versionFromBuffer) {
      int numBytes = indexInput.readByte();
      int size = indexInput.readInt();
      long currentPos = indexInput.getFilePointer();
      long refreshPos = currentPos + size;
      IndexInput indexInputToUse = indexInput.slice(currentPos, size);
      indexInput.seek(refreshPos);
      return new VSizeIndexed(indexInputToUse, numBytes);
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

  public WritableSupplier<IndexedMultivalue<IndexedInts>> asWritableSupplier()
  {
    return new VSizeIndexedSupplier(this);
  }

  public static class VSizeIndexedSupplier implements WritableSupplier<IndexedMultivalue<IndexedInts>>
  {
    final VSizeIndexed delegate;

    public VSizeIndexedSupplier(VSizeIndexed delegate)
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
    public IndexedMultivalue<IndexedInts> get()
    {
      return delegate;
    }
  }
}
