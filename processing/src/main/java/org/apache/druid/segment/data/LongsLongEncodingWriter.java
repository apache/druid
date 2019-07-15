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

import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongsLongEncodingWriter implements CompressionFactory.LongEncodingWriter
{

  private final ByteBuffer orderBuffer;
  private final ByteOrder order;
  @Nullable
  private ByteBuffer outBuffer = null;
  @Nullable
  private OutputStream outStream = null;

  public LongsLongEncodingWriter(ByteOrder order)
  {
    this.order = order;
    orderBuffer = ByteBuffer.allocate(Long.BYTES);
    orderBuffer.order(order);
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    outStream = null;
    outBuffer = buffer;
    // this order change is safe as the buffer is passed in and allocated in BlockLayoutColumnarLongsSerializer, and
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
  public void flush()
  {
  }

  @Override
  public void putMeta(ByteBuffer metaOut, CompressionStrategy strategy)
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
    return bytesPerBlock / Long.BYTES;
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Long.BYTES;
  }
}
