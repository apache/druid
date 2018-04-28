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

import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableIntegerCODEC;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Integer form encoder using {@link <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>}. Any
 * {@link SkippableIntegerCODEC} should work, but currently only {@link me.lemire.integercompression.FastPFOR} is
 * setup as a known encoding in {@link IntCodecs} as {@link IntCodecs#FASTPFOR}.
 *
 * layout:
 * | header (byte) | encoded values  (numOutputInts * Integer.BYTES) |
 */
public final class LemireIntFormEncoder extends BaseIntFormEncoder
{
  private final NonBlockingPool<SkippableIntegerCODEC> codecPool;
  private final byte header;
  private final String name;
  private int numOutputInts;

  public LemireIntFormEncoder(
      byte logValuesPerChunk,
      byte header,
      String name,
      ByteOrder byteOrder
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.header = header;
    this.name = name;
    this.codecPool = CompressedPools.getShapeshiftLemirePool(header, logValuesPerChunk);
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    try (ResourceHolder<int[]> tmpHolder = CompressedPools.getShapeshiftIntsEncodedValuesArray(logValuesPerChunk)) {
      final int[] encodedValuesTmp = tmpHolder.get();
      numOutputInts = doEncode(values, encodedValuesTmp, numValues);
      return numOutputInts * Integer.BYTES;
    }
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    try (ResourceHolder<int[]> tmpHolder = CompressedPools.getShapeshiftIntsEncodedValuesArray(logValuesPerChunk)) {
      final int[] encodedValuesTmp = tmpHolder.get();
      numOutputInts = doEncode(values, encodedValuesTmp, numValues);
      for (int i = 0; i < numOutputInts; i++) {
        valuesOut.write(toBytes(encodedValuesTmp[i]));
      }
    }
  }

  @Override
  public byte getHeader()
  {
    return header;
  }

  @Override
  public String getName()
  {
    return name;
  }

  private int doEncode(int[] values, int[] encodedValuesTmp, final int numValues)
  {
    final IntWrapper inPos = new IntWrapper(0);
    final IntWrapper outPos = new IntWrapper(0);

    try (ResourceHolder<SkippableIntegerCODEC> codecHolder = codecPool.take()) {
      final SkippableIntegerCODEC codec = codecHolder.get();
      codec.headlessCompress(values, inPos, numValues, encodedValuesTmp, outPos);
    }

    if (inPos.get() != numValues) {
      throw new ISE("Expected to compress[%d] ints, but read[%d]", numValues, inPos.get());
    }

    return outPos.get();
  }
}
