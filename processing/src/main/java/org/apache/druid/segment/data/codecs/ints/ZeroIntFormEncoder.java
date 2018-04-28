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

import java.nio.ByteOrder;

/**
 * Encoding optimization used if all values are the same within a chunk are zero, using 1 byte total, including
 * the header.
 *
 * layout:
 * | header: IntCodecs.ZERO (byte) |
 */
public class ZeroIntFormEncoder extends BaseIntFormEncoder
{
  public ZeroIntFormEncoder(byte logValuesPerChunk, ByteOrder byteOrder)
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
    if (metrics.isZero()) {
      return 0;
    }
    return Integer.MAX_VALUE;
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    // do nothing!
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.ZERO;
  }

  @Override
  public String getName()
  {
    return "zero";
  }
}
