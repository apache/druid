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

import com.google.common.base.Supplier;
import com.google.common.primitives.Floats;
import io.druid.java.util.common.IOE;
import io.druid.segment.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

public class EntireLayoutIndexedFloatSupplier implements Supplier<IndexedFloats>
{
  private final int totalSize;
  private FloatBuffer buffer;
  private IndexInput indexInput;
  private boolean isIIVersion;
  private ByteOrder byteOrder;

  public EntireLayoutIndexedFloatSupplier(int totalSize, ByteBuffer fromBuffer, ByteOrder order)
  {
    this.totalSize = totalSize;
    this.buffer = fromBuffer.asReadOnlyBuffer().order(order).asFloatBuffer();
    this.indexInput = null;
    this.isIIVersion = false;
    this.byteOrder = order;
  }


  public EntireLayoutIndexedFloatSupplier(int totalSize, IndexInput fromII, ByteOrder order)
  {
    try {
      this.totalSize = totalSize;
      this.buffer = null;
      this.indexInput = fromII.duplicate();
      this.isIIVersion = true;
      this.byteOrder = order;
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }


  @Override
  public IndexedFloats get()
  {
    return new EntireLayoutIndexedFloats();
  }

  private class EntireLayoutIndexedFloats implements IndexedFloats
  {

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public float get(int index)
    {
      if (!isIIVersion) {
        return buffer.get(buffer.position() + index);
      } else {
        return getIIV(index);
      }
    }

    /**
     * as short for entire layout is written according to specific byte order,
     * but randomAccessInput use big-endian encoding,so here we write this specifically.
     *
     * @param index
     *
     * @return
     */
    private float getIIV(int index)
    {
      try {
        synchronized (indexInput) {
          int currentPos = (int) indexInput.getFilePointer();
          int ix = index << 2;
          int refreshPos = currentPos + ix;
          indexInput.seek(refreshPos);
          ByteBuffer byteBuffer = ByteBuffer.allocate(Floats.BYTES);
          byteBuffer.order(byteOrder);
          indexInput.readBytes(byteBuffer.array(), 0, Floats.BYTES);
          indexInput.seek(currentPos);
          float value = byteBuffer.getFloat();
          return value;
        }
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }


    @Override
    public void fill(int index, float[] toFill)
    {
      if (totalSize - index < toFill.length) {
        throw new IndexOutOfBoundsException(
            String.format(
                "Cannot fill array of size[%,d] at index[%,d].  Max size[%,d]", toFill.length, index, totalSize
            )
        );
      }
      for (int i = 0; i < toFill.length; i++) {
        toFill[i] = get(index + i);
      }
    }

    @Override
    public String toString()
    {
      return "EntireCompressedIndexedFloats_Anonymous{" +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close()
    {
    }
  }
}
