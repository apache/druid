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

import org.apache.druid.segment.data.ShapeShiftingColumnarInts;
import org.apache.druid.segment.data.codecs.BaseFormDecoder;
import org.apache.druid.segment.data.codecs.DirectFormDecoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Byte packed integer decoder based on
 * {@link org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier} implementations
 *
 * layout:
 * | header: IntCodecs.BYTEPACK (byte) | numBytes (byte) | encoded values  (numValues * numBytes) |
 */
public final class BytePackedIntFormDecoder extends BaseFormDecoder<ShapeShiftingColumnarInts>
    implements DirectFormDecoder<ShapeShiftingColumnarInts>
{
  public static final int BIG_ENDIAN_INT_24_SHIFT = Integer.SIZE - 24;
  public static final int LITTLE_ENDIAN_INT_24_MASK = (int) ((1L << 24) - 1);

  public BytePackedIntFormDecoder(final byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  /**
   * Eagerly get all values into value array of shapeshifting int column
   *
   * @param columnarInts
   */
  @Override
  public void transform(ShapeShiftingColumnarInts columnarInts)
  {
    // metadata is always in base buffer at current chunk start offset
    final ByteBuffer metaBuffer = columnarInts.getBuffer();
    final int metaOffset = columnarInts.getCurrentChunkStartOffset();
    final byte numBytes = metaBuffer.get(metaOffset);
    columnarInts.setCurrentBytesPerValue(numBytes);
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.BYTEPACK;
  }

  @Override
  public int getMetadataSize()
  {
    return 1;
  }
}
