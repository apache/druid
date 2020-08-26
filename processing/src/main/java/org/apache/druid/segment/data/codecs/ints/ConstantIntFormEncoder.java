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
import java.nio.ByteOrder;

/**
 * Encoding optimization used if all values are the same within a chunk are constant, using 5 bytes total, including
 * the header.
 *
 * layout:
 * | header: IntCodecs.CONSTANT (byte) | constant value (int) |
 */
public class ConstantIntFormEncoder extends BaseIntFormEncoder
{
  public ConstantIntFormEncoder(final byte logValuesPerChunk, final ByteOrder byteOrder)
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
    if (metrics.isConstant()) {
      return Integer.BYTES;
    }
    return Integer.MAX_VALUE;
  }

  @Override
  public double getModifiedEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    if (metrics.isConstant()) {
      // count as 1 byte for sake of comparison, iow, never replace zero, but prefer this over 2 bpv rle
      return 1;
    }
    return Integer.MAX_VALUE;
  }


  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    valuesOut.write(toBytes(metrics.getMaxValue()));
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.CONSTANT;
  }

  @Override
  public String getName()
  {
    return "constant";
  }
}
