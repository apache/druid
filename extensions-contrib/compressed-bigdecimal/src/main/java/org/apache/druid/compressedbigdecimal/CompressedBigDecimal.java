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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.function.ToIntBiFunction;

/**
 * Mutable big decimal value that can be used to accumulate values without losing precision or reallocating memory.
 * This helps in revenue based calculations
 */
public abstract class CompressedBigDecimal extends Number implements Comparable<CompressedBigDecimal>
{
  protected static final long INT_MASK = 0x00000000ffffffffL;

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
   * @param rhs The object to accumulate
   * @return a reference to <b>this</b>
   */
  public CompressedBigDecimal accumulateSum(CompressedBigDecimal rhs)
  {
    checkScaleCompatibility(rhs);

    if (rhs.getArraySize() > getArraySize()) {
      throw new IllegalArgumentException("Right hand side too big to fit in the result value");
    }
    internalAdd(getArraySize(), this, CompressedBigDecimal::getArrayEntry, CompressedBigDecimal::setArrayEntry,
                rhs.getArraySize(), rhs, CompressedBigDecimal::getArrayEntry
    );
    return this;
  }

  public CompressedBigDecimal accumulateMax(CompressedBigDecimal rhs)
  {
    checkScaleCompatibility(rhs);

    if (compareTo(rhs) < 0) {
      setValue(rhs);
    }

    return this;
  }

  public CompressedBigDecimal accumulateMin(CompressedBigDecimal rhs)
  {
    checkScaleCompatibility(rhs);

    if (compareTo(rhs) > 0) {
      setValue(rhs);
    }

    return this;
  }

  private void checkScaleCompatibility(CompressedBigDecimal rhs)
  {
    Preconditions.checkArgument(
        rhs.getScale() == getScale(),
        "scales do not match: lhs [%s] vs rhs [%s]",
        getScale(),
        rhs.getScale()
    );
  }

  /**
   * copy the value from the rhs into this object
   * <p>
   * Note: implementations in subclasses are virtually identical, but specialized to allow for inlining of
   * element access. Callsites to the rhs's getArrayEntry should be monomorphic also (each subclass should only see the
   * same type it is)
   *
   * @param rhs a {@link CompressedBigDecimal} object
   */
  protected abstract void setValue(CompressedBigDecimal rhs);

  /**
   * Clear any value, resetting to zero. Scale is preserved at its original value.
   */
  public void reset()
  {
    for (int ii = 0; ii < getArraySize(); ++ii) {
      setArrayEntry(ii, 0);
    }
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
  static <R, S> void internalAdd(
      int llen,
      R lhs,
      ToIntBiFunction<R, Integer> lhsGet,
      ObjBiIntConsumer<R> lhsSet,
      int rlen,
      S rhs,
      ToIntBiFunction<S, Integer> rhsGet
  )
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
  private ByteArrayResult toByteArray()
  {
    int byteArrayLength = getArraySize() * 4;
    byte[] bytes = new byte[byteArrayLength];

    int byteIdx = 0;
    boolean isZero = true;
    for (int ii = getArraySize(); ii > 0; --ii) {
      int val = getArrayEntry(ii - 1);

      if (val != 0) {
        isZero = false;
      }

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
      return new ByteArrayResult(Arrays.copyOfRange(bytes, emptyBytes, byteArrayLength), isZero);
    }

    return new ByteArrayResult(bytes, isZero);
  }

  protected void setMinValue()
  {
    for (int i = 0; i < getArraySize(); i++) {
      if (i == getArraySize() - 1) {
        setArrayEntry(i, 0x80000000);
      } else {
        setArrayEntry(i, 0);
      }
    }
  }

  protected void setMaxValue()
  {
    for (int i = 0; i < getArraySize(); i++) {
      if (i == getArraySize() - 1) {
        setArrayEntry(i, 0x7FFFFFFF);
      } else {
        setArrayEntry(i, 0xFFFFFFFF);
      }
    }
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
   * @return a version of this object that is on heap. Returns this if already on-heap
   */
  public abstract CompressedBigDecimal toHeap();

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
    ByteArrayResult byteArrayResult = toByteArray();

    if (byteArrayResult.isZero) {
      return new BigDecimal(BigDecimal.ZERO.toBigInteger(), 0);
    } else {
      BigInteger bigInt = new BigInteger(byteArrayResult.bytes);

      return new BigDecimal(bigInt, scale);
    }
  }

  @Override
  public String toString()
  {
    BigDecimal bigDecimal = toBigDecimal();
    return bigDecimal.toString();
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

  public boolean isNegative()
  {
    return getArrayEntry(getArraySize() - 1) < 0;
  }

  public boolean isNonNegative()
  {
    return getArrayEntry(getArraySize() - 1) >= 0;
  }

  public boolean isZero()
  {
    boolean isZero = true;

    for (int i = getArraySize() - 1; i >= 0; i--) {
      if (getArrayEntry(i) != 0) {
        isZero = false;
        break;
      }
    }

    return isZero;
  }

  /**
   * Internal implementation if signum.
   * For the Provided Compressed big decimal value it checks and returns
   * -1 if Negative
   * 0 if Zero
   * 1 if Positive
   *
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

  @Override
  public int compareTo(CompressedBigDecimal o)
  {
    return compareTo(o, false);
  }

  public int compareTo(CompressedBigDecimal o, boolean expectOptimized)
  {
    if (super.equals(o)) {
      return 0;
    } else if (getScale() == o.getScale()) {
      return directCompareCompressedBigDecimal(this, o);
    } else {
      if (expectOptimized) {
        throw new IAE("expected optimized path");
      }

      return this.toBigDecimal().compareTo(o.toBigDecimal());
    }
  }

  /**
   * performs a subtraction of lhs - rhs to compare elements
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private static int directCompareCompressedBigDecimal(CompressedBigDecimal lhs, CompressedBigDecimal rhs)
  {
    // this short-circuit serves two functions: 1. it speeds up comparison in +/- cases 2. it avoids the case of
    // overflow of positive - negative and negative - positive. p - p and n - n both fit in the given allotment of ints
    if (lhs.isNonNegative() && rhs.isNegative()) {
      return 1;
    } else if (lhs.isNegative() && rhs.isNonNegative()) {
      return -1;
    }

    int size = Math.max(lhs.getArraySize(), rhs.getArraySize());
    int[] result = new int[size];
    int borrow = 0;
    // for each argument, if it's negative, our extension will be -1/INT_MASK (all 1s). else, all 0s
    long lhsExtension = lhs.getArrayEntry(lhs.getArraySize() - 1) < 0 ? INT_MASK : 0;
    long rhsExtension = rhs.getArrayEntry(rhs.getArraySize() - 1) < 0 ? INT_MASK : 0;
    boolean nonZeroValues = false;

    for (int i = 0; i < size; i++) {
      // "dynamically" extend lhs/rhs if it's shorter than the other using extensions computed above
      long leftElement = i < lhs.getArraySize() ? (INT_MASK & lhs.getArrayEntry(i)) : lhsExtension;
      long rightElement = i < rhs.getArraySize() ? (INT_MASK & rhs.getArrayEntry(i)) : rhsExtension;
      long resultElement = leftElement - rightElement - borrow;

      borrow = 0;

      if (resultElement < 0) {
        borrow = 1;
        resultElement += 1L << 32;
      }

      result[i] = (int) resultElement;

      if (!nonZeroValues && resultElement != 0) {
        nonZeroValues = true;
      }
    }

    int signum = 0;

    if (nonZeroValues) {
      signum = result[size - 1] < 0 ? -1 : 1;
    }

    return signum;
  }

  @Override
  public int hashCode()
  {
    return toBigDecimal().hashCode();
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof CompressedBigDecimal && toBigDecimal().equals(((CompressedBigDecimal) obj).toBigDecimal());
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

  private static class ByteArrayResult
  {
    private final byte[] bytes;
    private final boolean isZero;

    public ByteArrayResult(byte[] bytes, boolean isZero)
    {
      this.bytes = bytes;
      this.isZero = isZero;
    }
  }
}
