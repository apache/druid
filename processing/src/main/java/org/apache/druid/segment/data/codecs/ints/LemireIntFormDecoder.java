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
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.ShapeShiftingColumn;
import org.apache.druid.segment.data.ShapeShiftingColumnarInts;
import org.apache.druid.segment.data.codecs.ArrayFormDecoder;
import org.apache.druid.segment.data.codecs.BaseFormDecoder;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Integer form decoder using {@link <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>}. Any
 * {@link SkippableIntegerCODEC} will work, but currently only {@link me.lemire.integercompression.FastPFOR} is
 * setup as a known encoding in {@link IntCodecs} as {@link IntCodecs#FASTPFOR}.
 * Eagerly decodes all values to {@link ShapeShiftingColumnarInts#decodedValues}.
 *
 * layout:
 * | header (byte) | encoded values  (numOutputInts * Integer.BYTES) |
 */
public final class LemireIntFormDecoder extends BaseFormDecoder<ShapeShiftingColumnarInts>
    implements ArrayFormDecoder<ShapeShiftingColumnarInts>
{
  private static final Unsafe unsafe = ShapeShiftingColumn.getTheUnsafe();
  private final NonBlockingPool<SkippableIntegerCODEC> codecPool;
  private final byte header;

  public LemireIntFormDecoder(
      byte logValuesPerChunk,
      byte header,
      ByteOrder byteOrder
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.header = header;
    this.codecPool = CompressedPools.getShapeshiftLemirePool(header, logValuesPerChunk);
  }

  /**
   * Eagerly decode all values into value array of shapeshifting int column
   *
   * @param columnarInts
   */
  @Override
  public void transform(ShapeShiftingColumnarInts columnarInts)
  {
    final int numValues = columnarInts.getCurrentChunkNumValues();
    final int startOffset = columnarInts.getCurrentValuesStartOffset();
    final int endOffset = startOffset + columnarInts.getCurrentChunkSize();
    final ByteBuffer buffer = columnarInts.getCurrentValueBuffer();
    final int[] decodedChunk = columnarInts.getDecodedValues();

    final int chunkSizeBytes = endOffset - startOffset;

    // todo: needed?
    //CHECKSTYLE.OFF: Regexp
//    if (chunkSizeBytes % Integer.BYTES != 0) {
//      throw new ISE(
//          "Expected to read a whole number of integers, but got[%d] to [%d] for chunk",
//          startOffset,
//          endOffset
//      );
//    }
    //CHECKSTYLE.ON: Regexp

    // Copy chunk into an int array.
    final int chunkSizeAsInts = chunkSizeBytes >> 2;

    try (
        ResourceHolder<int[]> tmpHolder = CompressedPools.getShapeshiftIntsEncodedValuesArray(logValuesPerChunk);
        ResourceHolder<SkippableIntegerCODEC> codecHolder = codecPool.take()
    ) {
      final int[] tmp = tmpHolder.get();
      final SkippableIntegerCODEC codec = codecHolder.get();

      if (buffer.isDirect() && byteOrder.equals(ByteOrder.nativeOrder())) {
        long addr = ((DirectBuffer) buffer).address() + startOffset;
        for (int i = 0; i < chunkSizeAsInts; i++, addr += Integer.BYTES) {
          tmp[i] = unsafe.getInt(addr);
        }
      } else {
        for (int i = 0, bufferPos = startOffset; i < chunkSizeAsInts; i += 1, bufferPos += Integer.BYTES) {
          tmp[i] = buffer.getInt(bufferPos);
        }
      }

      // Decompress the chunk.
      final IntWrapper inPos = new IntWrapper(0);
      final IntWrapper outPos = new IntWrapper(0);

      // this will unpack encodedValuesTmp to decodedValues
      codec.headlessUncompress(
          tmp,
          inPos,
          chunkSizeAsInts,
          decodedChunk,
          outPos,
          numValues
      );
    }

    // todo: needed?
    // Sanity checks.
    //CHECKSTYLE.OFF: Regexp
//    if (inPos.get() != chunkSizeAsInts) {
//      throw new ISE(
//          "Expected to read[%d] ints but actually read[%d]",
//          chunkSizeAsInts,
//          inPos.get()
//      );
//    }
//
//    if (outPos.get() != numValues) {
//      throw new ISE("Expected to get[%d] ints but actually got[%d]", numValues, outPos.get());
//    }
    //CHECKSTYLE.ON: Regexp
  }

  @Override
  public byte getHeader()
  {
    return header;
  }
}
