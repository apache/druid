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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.segment.data.IndexedInts;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.function.ToIntBiFunction;

/**
 * Utility opertaions for accumlation.
 */
public class Utils
{
  /**
   * Accumulate (add) the passed in value into the current total. This
   * modifies the value of the current object. The scale of the BigDecimal is adjusted to match
   * the current accumulating scale. If the value being accumulated has a larger underlying array
   * than this value (the result), then the higher order bits are dropped, similar to
   * what happens when adding a long to an int and storing the result in an int.
   *
   * @param <S> Type of CompressedBigDecimal into which to accumulate
   * @param lhs The object into which to accumulate
   * @param rhs The object to accumulate
   * @return a reference to <b>this</b>
   */
  public static <S extends CompressedBigDecimal<S>>
      CompressedBigDecimal<S> accumulate(CompressedBigDecimal<S> lhs, BigDecimal rhs)
  {
    CompressedBigDecimal<ArrayCompressedBigDecimal> abd =
        new ArrayCompressedBigDecimal(rhs.setScale(lhs.getScale()));
    return lhs.accumulate(abd);
  }

  /**
   * Accumulate (add) the passed in value into the current total. This
   * modifies the value of the current object. Accumulation requires that
   * the two numbers have the same scale, but does not require that they are
   * of the same size. If the value being accumulated has a larger underlying array
   * than this value (the result), then the higher order bits are dropped, similar to
   * what happens when adding a long to an int and storing the result in an int.
   *
   * @param <S>      Type of CompressedBigDecimal into which to accumulate
   * @param lhs      The object into which to accumulate
   * @param rhs      The object to accumulate
   * @param rhsScale The scale to apply to the long being accumulated
   * @return a reference to <b>this</b>
   */
  public static <S extends CompressedBigDecimal<S>>
      CompressedBigDecimal<S> accumulate(CompressedBigDecimal<S> lhs, long rhs, int rhsScale)
  {
    CompressedBigDecimal<ArrayCompressedBigDecimal> abd = new ArrayCompressedBigDecimal(rhs, rhsScale);
    return lhs.accumulate(abd);
  }

  /**
   * Accumulate using IndexedInts read from Druid's segment file.
   *
   * @param <S>      Type of CompressedBigDecimal into which to accumulate
   * @param lhs      The object into which to accumulate
   * @param rhs      IndexedInts representing array of magnitude values
   * @param rhsScale the scale
   * @return a reference to <b>this</b>
   */
  public static <S extends CompressedBigDecimal<S>>
      CompressedBigDecimal<S> accumulate(CompressedBigDecimal<S> lhs, IndexedInts rhs, int rhsScale)
  {
    if (rhs.size() > lhs.getArraySize()) {
      throw new IllegalArgumentException("Right hand side too big to fit in the result value");
    }
    CompressedBigDecimal.internalAdd(lhs.getArraySize(), lhs, CompressedBigDecimal::getArrayEntry,
        CompressedBigDecimal::setArrayEntry, rhs.size(), rhs, IndexedInts::get);
    return lhs;
  }

  /**
   * Accumulate using ByteBuffers for Druid BufferAggregator.
   *
   * @param buf      The byte buffer that containes the result to accumlate into
   * @param pos      The initial position within the buffer
   * @param lhsSize  The array size of the left
   * @param lhsScale The scale of the left
   * @param rhs      the right side to accumlate
   */
  public static void accumulate(ByteBuffer buf, int pos, int lhsSize, int lhsScale, CompressedBigDecimal<?> rhs)
  {
    if (rhs.getArraySize() > lhsSize) {
      throw new IllegalArgumentException("Right hand side too big to fit in the result value");
    }
    BufferAccessor accessor = BufferAccessor.prepare(pos);
    CompressedBigDecimal.internalAdd(lhsSize, buf, accessor, accessor,
        rhs.getArraySize(), rhs, CompressedBigDecimal::getArrayEntry);
  }

  /**
   * Returns a {@code CompressedBigDecimal} whose scale is moderated as per the default scale.
   *
   * @param <S> Type of CompressedBigDecimal to scale
   * @param val The value to scale up
   * @return Scaled up compressedBigDecimal
   */
  public static <S extends CompressedBigDecimal<S>>
      CompressedBigDecimal<ArrayCompressedBigDecimal> scaleUp(CompressedBigDecimal<S> val)
  {
    return new ArrayCompressedBigDecimal(
        val.toBigDecimal().setScale(CompressedBigDecimalAggregatorFactory.DEFAULT_SCALE, BigDecimal.ROUND_UP)
    );
  }

  /**
   * Helper class that maintains a cache of thread local objects that can be used to access
   * a ByteBuffer in {@link Utils#accumulate(ByteBuffer, int, int, int, CompressedBigDecimal)}.
   */
  private static class BufferAccessor implements ToIntBiFunction<ByteBuffer, Integer>, ObjBiIntConsumer<ByteBuffer>
  {
    private static ThreadLocal<BufferAccessor> cache = new ThreadLocal<BufferAccessor>()
    {
      @Override
      protected BufferAccessor initialValue()
      {
        return new BufferAccessor();
      }
    };

    private int position = 0;

    /**
     * Initialized the BufferAccessor with the location that should be used for access.
     *
     * @param position position within the buffer
     * @return An initialized BufferAccessor
     */
    public static BufferAccessor prepare(int position)
    {
      BufferAccessor accessor = cache.get();
      accessor.position = position;
      return accessor;
    }

    /* (non-Javadoc)
     * @see org.apache.druid.compressedbigdecimal.ObjBiIntConsumer#accept(java.lang.Object, int, int)
     */
    @Override
    public void accept(ByteBuffer buf, int idx, int val)
    {
      buf.putInt(position + idx * Integer.BYTES, val);
    }

    /* (non-Javadoc)
     * @see java.util.function.ToIntBiFunction#applyAsInt(java.lang.Object, java.lang.Object)
     */
    @Override
    public int applyAsInt(ByteBuffer buf, Integer idx)
    {
      return buf.getInt(position + idx * Integer.BYTES);
    }
  }
}
