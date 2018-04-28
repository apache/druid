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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.codecs.ArrayFormDecoder;
import org.apache.druid.segment.data.codecs.CompressedFormDecoder;
import org.apache.druid.segment.data.codecs.FormDecoder;
import org.apache.druid.segment.data.codecs.ints.BytePackedIntFormDecoder;
import sun.misc.Unsafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ShapeShiftingColumnarInts extends ShapeShiftingColumn<ShapeShiftingColumnarInts> implements ColumnarInts
{
  public static final byte VERSION = 0x4; // todo: idk..

  protected static final Unsafe unsafe = getTheUnsafe();

  protected final GetIntBuffer getInt24;
  protected final GetIntUnsafe getInt24Unsafe;
  ResourceHolder<int[]> decodedValuesHolder;

  private final Supplier<int[]> decodedValuesSupplier;

  protected int[] tmp;
  protected int[] decodedValues;
  protected DecodeIndex currentForm;
  protected int currentBytesPerValue = 4;
  protected int currentConstant = 0;

  public ShapeShiftingColumnarInts(
      ShapeShiftingColumnData sourceData,
      Byte2ObjectMap<FormDecoder<ShapeShiftingColumnarInts>> decoders
  )
  {
    super(sourceData, decoders);
    this.decodedValuesSupplier = Suppliers.memoize(() -> {
      decodedValuesHolder = CompressedPools.getShapeshiftIntsDecodedValuesArray(logValuesPerChunk);
      return decodedValuesHolder.get();
    });

    getInt24 = byteOrder.equals(ByteOrder.LITTLE_ENDIAN)
                      ? (_buffer, pos) -> _buffer.getInt(pos) & BytePackedIntFormDecoder.LITTLE_ENDIAN_INT_24_MASK
                      : (_buffer, pos) -> _buffer.getInt(pos) >>> BytePackedIntFormDecoder.BIG_ENDIAN_INT_24_SHIFT;
    getInt24Unsafe = byteOrder.equals(ByteOrder.LITTLE_ENDIAN)
                            ? (pos) -> unsafe.getInt(pos) & BytePackedIntFormDecoder.LITTLE_ENDIAN_INT_24_MASK
                            : (pos) -> unsafe.getInt(pos) >>> BytePackedIntFormDecoder.BIG_ENDIAN_INT_24_SHIFT;
  }

  @Override
  public int size()
  {
    return numValues;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // todo: idk
    super.inspectRuntimeShape(inspector);
    inspector.visit("decodedValues", decodedValuesSupplier);
  }

  @Override
  public void close() throws IOException
  {
    super.close();
    if (decodedValuesHolder != null) {
      decodedValuesHolder.close();
    }
  }

  @Override
  public int get(final int index)
  {
    final int desiredChunk = index >> logValuesPerChunk;

    if (desiredChunk != currentChunk) {
      loadChunk(desiredChunk);
    }

    return currentForm.decode(index & chunkIndexMask);
  }

  /**
   * integer array sized to number of values, to allow {@link FormDecoder} a place for fully
   * decoded values upon transformation
   *
   * @return
   */
  public final int[] getDecodedValues()
  {
    return decodedValues = decodedValuesSupplier.get();
  }

  /**
   * current 'constant' value, for constant chunk transformations
   *
   * @return
   */
  public final int getCurrentConstant()
  {
    return currentConstant;
  }

  /**
   * Allows {@link FormDecoder} to set current 'constant' value during a transformation.
   *
   * @param currentConstant
   */
  public final void setCurrentConstant(int currentConstant)
  {
    this.currentConstant = currentConstant;
  }

  /**
   * Get current number of bytes used per value for random access transformations.
   *
   * @return
   */
  public int getCurrentBytesPerValue()
  {
    return currentBytesPerValue;
  }

  /**
   * Allows {@link FormDecoder} to set current number of bytes for value, for random access transformations
   *
   * @param currentBytesPerValue
   */
  public void setCurrentBytesPerValue(int currentBytesPerValue)
  {
    this.currentBytesPerValue = currentBytesPerValue;
  }

  /**
   * Transform {@link ShapeShiftingColumnarInts} to the form of the requested chunk, which may either be eagerly
   * decoded entirely to {@link ShapeShiftingColumnarInts#decodedValuesSupplier} with values retrieved by
   * {@link ShapeShiftingColumnarInts#decodeBlockForm(int)}, or randomly accessible, which may set
   * {@link ShapeShiftingColumnarInts#currentValuesAddress}, {@link ShapeShiftingColumnarInts#currentValuesStartOffset},
   * {@link ShapeShiftingColumnarInts#currentBytesPerValue}, {@link ShapeShiftingColumnarInts#currentConstant} and be
   * decoded by {@link ShapeShiftingColumnarInts#decodeBufferForm(int)}.
   *
   * @param nextForm
   */
  @Override
  public void transform(FormDecoder<ShapeShiftingColumnarInts> nextForm)
  {
    currentBytesPerValue = 4;
    currentConstant = 0;

    nextForm.transform(this);
    if (nextForm instanceof ArrayFormDecoder) {
      currentForm = this::decodeBlockForm;
    } else if (!(nextForm instanceof CompressedFormDecoder)) {
      if (getCurrentValueBuffer().isDirect() && byteOrder.equals(ByteOrder.nativeOrder())) {
        currentForm = this::decodeUnsafeForm;
      } else {
        currentForm = this::decodeBufferForm;
      }
    }
  }

  /**
   * get value at index produced {@link FormDecoder} transformation
   *
   * @param index masked index into the chunk array (index & {@link ShapeShiftingColumnarInts#chunkIndexMask})
   *
   * @return decoded row value at index
   */
  private int decodeBlockForm(int index)
  {
    return decodedValues[index];
  }

  /**
   * get value (unsafe) at index produced by {@link FormDecoder} transformation
   *
   * @param index masked index into the chunk array (index & {@link ShapeShiftingColumnarInts#chunkIndexMask})
   *
   * @return decoded row value at index
   */
  private int decodeUnsafeForm(int index)
  {
    final long pos = currentValuesAddress + (index * currentBytesPerValue);
    switch (currentBytesPerValue) {
      case 1:
        return unsafe.getByte(pos) & 0xFF;
      case 2:
        return unsafe.getShort(pos) & 0xFFFF;
      case 3:
        return getInt24Unsafe.getInt(pos);
      case 4:
        return unsafe.getInt(pos);
      default:
        return currentConstant;
    }
  }

  /**
   * get value at index produced by {@link FormDecoder} transformation
   *
   * @param index masked index into the chunk array (index & {@link ShapeShiftingColumnarInts#chunkIndexMask})
   *
   * @return decoded row value at index
   */
  private int decodeBufferForm(int index)
  {
    final int pos = getCurrentValuesStartOffset() + (index * currentBytesPerValue);
    final ByteBuffer buffer = getCurrentValueBuffer();
    switch (currentBytesPerValue) {
      case 1:
        return buffer.get(pos) & 0xFF;
      case 2:
        return buffer.getShort(pos) & 0xFFFF;
      case 3:
        return getInt24.getInt(buffer, pos);
      case 4:
        return buffer.getInt(pos);
      default:
        return currentConstant;
    }
  }

  @FunctionalInterface
  public interface DecodeIndex
  {
    int decode(int index);
  }

  @FunctionalInterface
  protected interface GetIntUnsafe
  {
    int getInt(long index);
  }

  @FunctionalInterface
  protected interface GetIntBuffer
  {
    int getInt(ByteBuffer buffer, int index);
  }
}
