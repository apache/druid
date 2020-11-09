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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.function.ToIntBiFunction;

/**
 * Mutable big decimal value that can be used to accumulate values without losing precision or reallocating memory.
 *
 * @param <T> Type of actual derived class that contains the underlying data
 */
@SuppressWarnings("serial")
public abstract class CompressedBigDecimal<T extends CompressedBigDecimal<T>> extends Number
    implements Comparable<CompressedBigDecimal<T>>
{

  private static final long INT_MASK = 0x00000000ffffffffL;

  private final int scale;

  /**
   * Construct an CompressedBigDecimal by specifying the scale, array size, and a function to get
   * and put array values.
   *
   * @param scale scale of the compressed big decimal
   */
  protected CompressedBigDecimal(int scale)
  {
    this.scale = scale;
  }


  /**
   * Accumulate (add) the passed in value into the current total. This
   * modifies the value of the current object. Accumulation requires that
   * the two numbers have the same scale, but does not require that they are
   * of the same size. If the value being accumulated has a larger underlying array
   * than this value (the result), then the higher order bits are dropped, similar to
   * what happens when adding a long to an int and storing the result in an int.
   *
   * @param <S> type of compressedbigdecimal to accumulate
   * @param rhs The object to accumulate
   * @return a reference to <b>this</b>
   */
  public <S extends CompressedBigDecimal<S>> CompressedBigDecimal<T> accumulate(CompressedBigDecimal<S> rhs)
  {
    if (rhs.scale != scale) {
      throw new IllegalArgumentException("Cannot accumulate MutableBigDecimals with differing scales");
    }
    if (rhs.getArraySize() > getArraySize()) {
      throw new IllegalArgumentException("Right hand side too big to fit in the result value");
    }
    internalAdd(getArraySize(), this, CompressedBigDecimal::getArrayEntry, CompressedBigDecimal::setArrayEntry,
        rhs.getArraySize(), rhs, CompressedBigDecimal::getArrayEntry);
    return this;
  }

  /**
   * Clear any accumulated value, resetting to zero. Scale is preserved at its original value.
   *
   * @return this
   */
  public CompressedBigDecimal<T> reset()
  {
    for (int ii = 0; ii < getArraySize(); ++ii) {
      setArrayEntry(ii, 0);
    }
    return this;
  }

  /**
   * Accumulate values into this mutable value. Values to be accumulated
   * must have the same scale as the result. The incoming value may have
   * been more precision than the result. If so,  the upper bits are truncated,
   * similar to what happens when a long is assigned to an int.
   *
   * @param <R>    type of the object containing the lhs array
   * @param <S>    type of obejct containing the rhs array
   * @param llen   the underlying left array size
   * @param lhs    the object containing the left array data
   * @param lhsGet method reference to get an underlying left value
   * @param lhsSet method reference to set an underlying left value
   * @param rlen   the underlying right array size
   * @param rhs    the object containing the right array data
   * @param rhsGet method reference to get an underlying right value
   */
  static <R, S> void internalAdd(int llen, R lhs, ToIntBiFunction<R, Integer> lhsGet, ObjBiIntConsumer<R> lhsSet,
                                 int rlen, S rhs, ToIntBiFunction<S, Integer> rhsGet)
  {
    int commonLen = Integer.min(llen, rlen);
    long carry = 0;
    long sum;
    // copy the common part
    for (int ii = 0; ii < commonLen; ++ii) {
      sum = (INT_MASK & lhsGet.applyAsInt(lhs, ii)) + (INT_MASK & rhsGet.applyAsInt(rhs, ii)) + carry;
      lhsSet.accept(lhs, ii, (int) sum);
      carry = sum >>> 32;
    }

    long signExtension = signumInternal(rlen, rhs, rhsGet) < 0 ? INT_MASK : 0;

    // for the remaining portion of the lhs that didn't have matching
    // rhs values, just propagate any necessary carry and sign extension
    for (int ii = commonLen; ii < llen && (carry != 0 || signExtension != 0); ++ii) {
      sum = (INT_MASK & lhsGet.applyAsInt(lhs, ii)) + signExtension + carry;
      lhsSet.accept(lhs, ii, (int) sum);
      carry = sum >>> 32;
    }
    // don't do anything with remaining rhs. That value is lost due to overflow.
  }

  /**
   * Get a byte array representing the magnitude of this value,
   * formatted for use by {@link BigInteger#BigInteger(byte[])}.
   *
   * @return the byte array for use in BigInteger
   */
  private byte[] toByteArray()
  {
    int byteArrayLength = getArraySize() * 4;
    byte[] bytes = new byte[byteArrayLength];

    int byteIdx = 0;
    for (int ii = getArraySize(); ii > 0; --ii) {
      int val = getArrayEntry(ii - 1);
      bytes[byteIdx + 3] = (byte) val;
      val >>>= 8;
      bytes[byteIdx + 2] = (byte) val;
      val >>>= 8;
      bytes[byteIdx + 1] = (byte) val;
      val >>>= 8;
      bytes[byteIdx] = (byte) val;
      byteIdx += 4;
    }

    int leadingZeros = Integer.numberOfLeadingZeros(getArrayEntry(getArraySize() - 1));
    int emptyBytes = leadingZeros / 8;
    if (emptyBytes != 0) {
      if (emptyBytes == byteArrayLength || leadingZeros % 8 == 0) {
        // don't get rid of all the leading zeros if it is the complete number (array size must
        // be a minimum of 1) or if trimming the byte might change the sign of the value (first
        // one is on a byte boundary).
        emptyBytes--;
      }
      return Arrays.copyOfRange(bytes, emptyBytes, byteArrayLength);
    }

    return bytes;
  }

  /**
   * Return the scale of the value.
   *
   * @return the scale
   */
  public int getScale()
  {
    return scale;
  }

  /**
   * Return the array size.
   *
   * @return the array size
   */
  protected abstract int getArraySize();

  /**
   * Get value from the array.
   *
   * @param idx the index
   * @return value from the array at that index
   */
  protected abstract int getArrayEntry(int idx);

  /**
   * Set value in the array.
   *
   * @param idx the index
   * @param val the value
   */
  protected abstract void setArrayEntry(int idx, int val);

  /**
   * Create a {@link BigDecimal} with the equivalent value to this
   * instance.
   *
   * @return the BigDecimal value
   */
  public BigDecimal toBigDecimal()
  {
    BigInteger bigInt = new BigInteger(toByteArray());
    return new BigDecimal(bigInt, scale);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    return toBigDecimal().toString();
  }

  /**
   * Returns the signum function of this {@code BigDecimal}.
   *
   * @return -1, 0, or 1 as the value of this {@code BigDecimal} is negative, zero, or positive.
   */
  public int signum()
  {
    return signumInternal(getArraySize(), this, CompressedBigDecimal::getArrayEntry);
  }

  /**
   * Internal implementation if signum.
   * For the Provided Compressed big decimal value it checks and returns
   * -1 if Negative
   * 0 if Zero
   * 1 if Positive
   * @param <S>     type of object containing the array
   * @param size    the underlying array size
   * @param rhs     object that contains the underlying array
   * @param valFunc method reference to get an underlying value
   * @return -1, 0, or 1 as the value of this {@code BigDecimal} is negative, zero, or positive.
   */
  protected static <S> int signumInternal(int size, S rhs, ToIntBiFunction<S, Integer> valFunc)
  {
    if (valFunc.applyAsInt(rhs, size - 1) < 0) {
      return -1;
    } else {
      int agg = 0;
      for (int ii = 0; ii < size; ++ii) {
        agg |= valFunc.applyAsInt(rhs, ii);
      }
      if (agg == 0) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(CompressedBigDecimal<T> o)
  {

    if (this.equals(o)) {
      return 0;
    }
    return this.toBigDecimal().compareTo(o.toBigDecimal());
  }

  /**
   * Returns the value of the specified number as an {@code int},
   * which may involve rounding or truncation.
   *
   * @return the numeric value represented by this object after conversion
   * to type {@code int}.
   */
  @Override
  public int intValue()
  {
    return toBigDecimal().setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
  }

  /**
   * Returns the value of the specified number as a {@code long},
   * which may involve rounding or truncation.
   *
   * @return the numeric value represented by this object after conversion
   * to type {@code long}.
   */
  @Override
  public long longValue()
  {
    return toBigDecimal().setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
  }

  /**
   * Returns the value of the specified number as a {@code float},
   * which may involve rounding.
   *
   * @return the numeric value represented by this object after conversion
   * to type {@code float}.
   */
  @Override
  public float floatValue()
  {
    return toBigDecimal().floatValue();
  }

  /**
   * Returns the value of the specified number as a {@code double},
   * which may involve rounding.
   *
   * @return the numeric value represented by this object after conversion
   * to type {@code double}.
   */
  @Override
  public double doubleValue()
  {
    return toBigDecimal().doubleValue();
  }
}
