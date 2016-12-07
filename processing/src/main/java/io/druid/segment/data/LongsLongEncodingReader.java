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

import com.google.common.primitives.Longs;
import io.druid.java.util.common.IOE;
import io.druid.segment.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 * LongBuffer and IndexInput are mutual exclusion when setting happens
 */
public class LongsLongEncodingReader implements CompressionFactory.LongEncodingReader
{
  private LongBuffer buffer;

  private IndexInput indexInput;

  private ByteOrder byteOrder;

  private boolean indexInputVersion;

  public LongsLongEncodingReader(ByteBuffer fromBuffer, ByteOrder order)
  {
    this.buffer = fromBuffer.asReadOnlyBuffer().order(order).asLongBuffer();
    this.indexInput = null;
    this.indexInputVersion = false;
  }

  public LongsLongEncodingReader(IndexInput indexInput, ByteOrder order)
  {
    try {
      this.indexInput = indexInput.duplicate();
      this.byteOrder = order;
      this.buffer = null;
      this.indexInputVersion = true;
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  private LongsLongEncodingReader(LongBuffer buffer)
  {
    this.buffer = buffer;
    this.indexInput = null;
    this.indexInputVersion = false;
  }

  private LongsLongEncodingReader(IndexInput indexInput)
  {
    this.indexInput = indexInput;
    this.buffer = null;
    this.indexInputVersion = true;
  }


  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    this.buffer = buffer.asLongBuffer();
  }

  @Override
  public void setIndexInput(IndexInput indexInput)
  {
    this.indexInput = indexInput;
  }

  @Override
  public long read(int index)
  {
    if (!indexInputVersion) {
      return buffer.get(buffer.position() + index);
    } else {
      return readLLFromII(index);
    }
  }

  //the reason why here not use rangdomAccess is that ,the IndexInput use big-endian to R/W ,but longlong encoding may use different l/b endian.
  private long readLLFromII(int index)
  {
    try {
      synchronized (indexInput) {
        int ix = index << 3;
        int initPos = (int) indexInput.getFilePointer();
        indexInput.seek(initPos + ix);
        ByteBuffer byteBuffer = ByteBuffer.allocate(Longs.BYTES);
        byteBuffer.order(byteOrder);
        indexInput.readBytes(byteBuffer.array(), 0, Longs.BYTES);
        long value = byteBuffer.getLong();
        indexInput.seek(initPos);
        return value;
      }
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Longs.BYTES;
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    if (!indexInputVersion) {
      return new LongsLongEncodingReader(buffer.duplicate());
    } else {
      try {
        return new LongsLongEncodingReader(indexInput.duplicate());
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }
}
