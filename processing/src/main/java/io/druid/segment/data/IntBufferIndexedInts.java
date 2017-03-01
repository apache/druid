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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import io.druid.collections.IntList;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 */
public class IntBufferIndexedInts implements IndexedInts, Comparable<IntBufferIndexedInts>
{
  public static ObjectStrategy<IntBufferIndexedInts> objectStrategy =
      new IntBufferIndexedIntsObjectStrategy();

  public static IntBufferIndexedInts fromArray(int[] array)
  {
    //TODO Replace this with int[] constructor in new Memory package. Till then use this workaround
    ByteBuffer bb = ByteBuffer.allocate(array.length*Ints.BYTES).order(ByteOrder.nativeOrder());
    for(int i = 0; i < array.length; i++){
      bb.putInt(array[i]);
    }
    return new IntBufferIndexedInts(new NativeMemory(bb));
  }

  public static IntBufferIndexedInts fromIntList(IntList intList)
  {
    final ByteBuffer buffer = ByteBuffer.allocate(intList.length() * Ints.BYTES);

    for (int i = 0; i < intList.length(); ++i) {
      buffer.putInt(i*Ints.BYTES, intList.get(i));
    }

    return new IntBufferIndexedInts(new NativeMemory(buffer));
  }

  private final Memory memory;

  public IntBufferIndexedInts(Memory memory)
  {
    this.memory = memory;
  }

  @Override
  public int size()
  {
    return (int)memory.getCapacity() / Ints.BYTES;
  }

  @Override
  public int get(int index)
  {
    return memory.getInt(index * Ints.BYTES);
  }

  public Memory getBuffer()
  {
    return memory;
  }

  @Override
  //TODO Remove this and implement Memory.compareTo in Memory
  public int compareTo(IntBufferIndexedInts o)
  {
    int n = (int)Math.min(this.getBuffer().getCapacity(), o.getBuffer().getCapacity())/Ints.BYTES;
    int i, j;
    for (i = 0, j = 0; i < n; i++, j++) {
      int cmp = Byte.compare(this.getBuffer().getByte(i), o.getBuffer().getByte(j));
      if (cmp != 0) {
        return cmp;
      }
    }
    return (int)((this.getBuffer().getCapacity()-i) - (o.getBuffer().getCapacity()-j));
  }

  @Override
  public IntIterator iterator()
  {
    return new IndexedIntsIterator(this);
  }

  private static class IntBufferIndexedIntsObjectStrategy implements ObjectStrategy<IntBufferIndexedInts>
  {
    @Override
    public Class<? extends IntBufferIndexedInts> getClazz()
    {
      return IntBufferIndexedInts.class;
    }

    @Override
    public IntBufferIndexedInts fromMemory(Memory memory)
    {
      return new IntBufferIndexedInts(memory);
    }

    @Override
    public byte[] toBytes(IntBufferIndexedInts val)
    {
      Memory memory = val.getBuffer();
      byte[] bytes = new byte[(int)memory.getCapacity()];
      memory.getByteArray(0, bytes, 0, bytes.length);

      return bytes;
    }

    @Override
    public int compare(IntBufferIndexedInts o1, IntBufferIndexedInts o2)
    {
      return Ordering.natural().nullsFirst().compare(o1, o2);
    }
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
}
