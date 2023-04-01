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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A compressed big decimal that holds its data with an embedded array.
 */
public class ArrayCompressedBigDecimal extends CompressedBigDecimal
{
  public static final CompressedBigDecimal ZERO_COMPRESSED_BIG_DECIMAL =
      new ArrayCompressedBigDecimal(BigDecimal.ZERO);

  private static final int BYTE_MASK = 0xff;

  private final int[] array;


  /**
   * Construct an AccumulatingBigDecimal using the referenced initial
   * value and scale.
   *
   * @param initialVal initial value
   * @param scale      the scale to use
   */
  public ArrayCompressedBigDecimal(long initialVal, int scale)
  {
    super(scale);
    this.array = new int[2];
    this.array[0] = (int) initialVal;
    this.array[1] = (int) (initialVal >>> 32);
  }

  /**
   * Construct an CompressedBigDecimal from an equivalent {@link BigDecimal}.
   * The passed value's unscaled number is converted into byte array and then it
   * compresses the unscaled number to array
   *
   * @param initialVal The BigDecimal value.
   */
  public ArrayCompressedBigDecimal(BigDecimal initialVal)
  {
    super(initialVal.scale());
    BigInteger unscaled = initialVal.unscaledValue();
    byte[] bytes = unscaled.toByteArray();
    int arrayLen = (bytes.length + 3) / 4;

    this.array = new int[arrayLen];

    if (initialVal.signum() == 0) {
      // initial value is 0. Nothing to copy
      return;
    }

    int bytesIdx = bytes.length;
    for (int ii = 0; ii < arrayLen; ++ii) {
      this.array[ii] =
          (BYTE_MASK & bytes[--bytesIdx]) |
          (bytesIdx != 0 ? (BYTE_MASK & bytes[--bytesIdx]) : (((int) bytes[0]) >> 8)) << 8 |
          (bytesIdx != 0 ? (BYTE_MASK & bytes[--bytesIdx]) : (((int) bytes[0]) >> 8)) << 16 |
          (bytesIdx != 0 ? (BYTE_MASK & bytes[--bytesIdx]) : (((int) bytes[0]) >> 8)) << 24;
    }
  }

  /**
   * Construct an CompressedBigDecimal that is a copy of the passed in value.
   *
   * @param initVal the initial value
   */
  public ArrayCompressedBigDecimal(CompressedBigDecimal initVal)
  {
    super(initVal.getScale());
    this.array = new int[initVal.getArraySize()];
    for (int ii = 0; ii < initVal.getArraySize(); ++ii) {
      this.array[ii] = initVal.getArrayEntry(ii);
    }
  }

  /**
   * Private constructor to build an CompressedBigDecimal that wraps
   * a passed in array of ints. The constructor takes ownership of the
   * array and its contents may be modified.
   *
   * @param array the initial magnitude
   * @param scale the scale
   */
  private ArrayCompressedBigDecimal(int[] array, int scale)
  {
    super(scale);
    this.array = array;
  }

  /**
   * Static method to construct an CompressedBigDecimal that wraps an
   * underlying array.
   *
   * @param array The array to wrap
   * @param scale The scale to use
   * @return An CompressedBigDecimal
   */
  public static ArrayCompressedBigDecimal wrap(int[] array, int scale)
  {
    return new ArrayCompressedBigDecimal(array, scale);
  }

  /**
   * Allocate a new CompressedBigDecimal with the specified size and scale and a value of 0
   *
   * @param size  size of the int array used for calculations
   * @param scale scale of the number
   * @return CompressedBigDecimal
   */
  public static ArrayCompressedBigDecimal allocateZero(int size, int scale)
  {
    int[] arr = new int[size];

    return new ArrayCompressedBigDecimal(arr, scale);
  }

  /**
   * Allocate a new CompressedBigDecimal with the specified size and scale and a value of "MIN_VALUE"
   *
   * @param size  size of the int array used for calculations
   * @param scale scale of the number
   * @return CompressedBigDecimal
   */
  public static ArrayCompressedBigDecimal allocateMin(int size, int scale)
  {
    int[] arr = new int[size];
    ArrayCompressedBigDecimal result = new ArrayCompressedBigDecimal(arr, scale);

    result.setMinValue();

    return result;
  }

  /**
   * Allocate a new CompressedBigDecimal with the specified size and scale and a value of "MAX_VALUE"
   *
   * @param size  size of the int array used for calculations
   * @param scale scale of the number
   * @return CompressedBigDecimal
   */
  public static ArrayCompressedBigDecimal allocateMax(int size, int scale)
  {
    int[] arr = new int[size];
    ArrayCompressedBigDecimal result = new ArrayCompressedBigDecimal(arr, scale);

    result.setMaxValue();

    return result;
  }

  @Override
  public CompressedBigDecimal toHeap()
  {
    return this;
  }

  /* (non-Javadoc)
   * @see org.apache.druid.compressedbigdecimal.CompressedBigDecimal#getArraySize()
   */
  @Override
  public int getArraySize()
  {
    return array.length;
  }

  /**
   * Package private access to internal array.
   *
   * @return the array
   */
  int[] getArray()
  {
    return array;
  }

  /**
   * Package private access to entry in internal array.
   *
   * @param idx index to retrieve
   * @return the entry
   */
  @Override
  protected int getArrayEntry(int idx)
  {
    return array[idx];
  }

  /**
   * Package private access to set entry in internal array.
   *
   * @param idx index to retrieve
   * @param val value to set
   */
  @Override
  protected void setArrayEntry(int idx, int val)
  {
    array[idx] = val;
  }

  @Override
  protected void setValue(CompressedBigDecimal rhs)
  {
    Preconditions.checkArgument(
        rhs.getArraySize() <= array.length,
        "lhs too small to store entry: lhs [%s] vs rhs [%s]",
        getArraySize(),
        rhs.getArraySize()
    );

    long extension = rhs.getArrayEntry(rhs.getArraySize() - 1) < 0 ? INT_MASK : 0L;

    for (int i = 0; i < array.length; i++) {
      long rhsElement;

      if (i < rhs.getArraySize()) {
        rhsElement = INT_MASK & rhs.getArrayEntry(i);
      } else {
        rhsElement = extension;
      }

      array[i] = (int) rhsElement;
    }

  }
}
