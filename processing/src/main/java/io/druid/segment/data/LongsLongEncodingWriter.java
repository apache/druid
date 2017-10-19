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
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongsLongEncodingWriter implements CompressionFactory.LongEncodingWriter
{

  private final ByteBuffer orderBuffer;
  private final ByteOrder order;
  private ByteBuffer outBuffer = null;
  private OutputStream outStream = null;

  public LongsLongEncodingWriter(ByteOrder order)
  {
    this.order = order;
    orderBuffer = ByteBuffer.allocate(Longs.BYTES);
    orderBuffer.order(order);
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    outStream = null;
    outBuffer = buffer;
    // this order change is safe as the buffer is passed in and allocated in BlockLayoutLongSupplierSerializer, and
    // is used only as a temporary storage to be written
    outBuffer.order(order);
  }

  @Override
  public void setOutputStream(WriteOutBytes output)
  {
    outBuffer = null;
    outStream = output;
  }

  @Override
  public void write(long value) throws IOException
  {
    if (outBuffer != null) {
      outBuffer.putLong(value);
    }
    if (outStream != null) {
      orderBuffer.rewind();
      orderBuffer.putLong(value);
      outStream.write(orderBuffer.array());
    }
  }

  @Override
  public void flush() throws IOException
  {
  }

  @Override
  public void putMeta(ByteBuffer metaOut, CompressionStrategy strategy) throws IOException
  {
    metaOut.put(strategy.getId());
  }

  @Override
  public int metaSize()
  {
    return 1;
  }

  @Override
  public int getBlockSize(int bytesPerBlock)
  {
    return bytesPerBlock / Longs.BYTES;
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Longs.BYTES;
  }
}
