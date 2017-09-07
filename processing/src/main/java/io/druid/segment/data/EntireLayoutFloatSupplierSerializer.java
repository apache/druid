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
import io.druid.io.Channels;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.output.OutputBytes;
import io.druid.output.OutputMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class EntireLayoutFloatSupplierSerializer implements FloatSupplierSerializer
{
  private final boolean isLittleEndian;
  private final OutputMedium outputMedium;
  private OutputBytes valuesOut;

  private int numInserted = 0;

  EntireLayoutFloatSupplierSerializer(OutputMedium outputMedium, ByteOrder order)
  {
    this.outputMedium = outputMedium;
    isLittleEndian = order.equals(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = outputMedium.makeOutputBytes();
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(float value) throws IOException
  {
    int valueBits = Float.floatToRawIntBits(value);
    // OutputBytes are always big-endian, so need to reverse bytes
    if (isLittleEndian) {
      valueBits = Integer.reverseBytes(valueBits);
    }
    valuesOut.writeInt(valueBits);
    ++numInserted;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return metaSize() + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    ByteBuffer meta = ByteBuffer.allocate(metaSize());
    meta.put(CompressedFloatsIndexedSupplier.version);
    meta.putInt(numInserted);
    meta.putInt(0);
    meta.put(CompressionStrategy.NONE.getId());
    meta.flip();

    Channels.writeFully(channel, meta);
    valuesOut.writeTo(channel);
  }

  private int metaSize()
  {
    return 1 + Ints.BYTES + Ints.BYTES + 1;
  }
}
