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

package org.apache.druid.segment.data.codecs.ints;

import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Unencoded integer encoder that writes full sized integer values as is.
 *
 * layout:
 * | header: IntCodecs.UNENCODED (byte) | values  (numValues * Integer.BYTES) |
 */
public class UnencodedIntFormEncoder extends CompressibleIntFormEncoder
{
  public UnencodedIntFormEncoder(byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    return numValues * Integer.BYTES;
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    WriteOutFunction writer = (value) -> valuesOut.write(toBytes(value));
    encodeValues(writer, values, numValues);
  }

  @Override
  public void encodeToBuffer(
      ByteBuffer buffer,
      int[] values,
      int numValues,
      IntFormMetrics metadata
  ) throws IOException
  {
    WriteOutFunction writer = (value) -> buffer.putInt(value);
    encodeValues(writer, values, numValues);
    buffer.flip();
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.UNENCODED;
  }

  @Override
  public String getName()
  {
    return "unencoded";
  }

  private void encodeValues(WriteOutFunction writer, int[] values, int numValues) throws IOException
  {
    for (int i = 0; i < numValues; i++) {
      writer.write(values[i]);
    }
  }
}
