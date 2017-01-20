/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections.bitmap;

import java.nio.ByteBuffer;

/**
 * This class is meant to represent a simple wrapper around a bitmap class.
 */
public interface MutableBitmap extends ImmutableBitmap
{
  /**
   * Empties the content of this bitmap.
   */
  public void clear();

  /**
   * Compute the bitwise-or of this bitmap with another bitmap. The current
   * bitmap is modified whereas the other bitmap is left intact.
   *
   * Note that the other bitmap should be of the same class instance.
   *
   * @param mutableBitmap other bitmap
   */
  public void or(MutableBitmap mutableBitmap);

  /**
   * Compute the bitwise-and of this bitmap with another bitmap. The current
   * bitmap is modified whereas the other bitmap is left intact.
   *
   * Note that the other bitmap should be of the same class instance.
   *
   * @param mutableBitmap other bitmap
   */
  public void and(MutableBitmap mutableBitmap);


  /**
   * Compute the bitwise-xor of this bitmap with another bitmap. The current
   * bitmap is modified whereas the other bitmap is left intact.
   *
   * Note that the other bitmap should be of the same class instance.
   *
   * @param mutableBitmap other bitmap
   */
  public void xor(MutableBitmap mutableBitmap);

  /**
   * Compute the bitwise-andNot of this bitmap with another bitmap. The current
   * bitmap is modified whereas the other bitmap is left intact.
   *
   * Note that the other bitmap should be of the same class instance.
   *
   * @param mutableBitmap other bitmap
   */
  public void andNot(MutableBitmap mutableBitmap);

  /**
   * Return the size in bytes for the purpose of serialization to a ByteBuffer.
   * Note that this is distinct from the memory usage.
   *
   * @return the total set in bytes
   */
  public int getSizeInBytes();

  /**
   * Add the specified integer to the bitmap. This is equivalent to setting the
   * ith bit to the value 1.
   *
   * @param entry integer to be added
   */
  public void add(int entry);

  /**
   * Remove the specified integer to the bitmap. This is equivalent to setting the
   * ith bit to the value 1.
   *
   * @param entry integer to be remove
   */
  public void remove(int entry);

  /**
   * Write out a serialized (Immutable) version of the bitmap to the ByteBuffer. We preprend
   * the serialized bitmap with a 4-byte int indicating the size in bytes. Thus
   * getSizeInBytes() + 4 bytes are written.
   *
   * (These 4 bytes are required by ConciseSet but not by RoaringBitmap.
   * Nevertheless, we always write them for the sake of simplicity, even if it
   * wastes 4 bytes in some instances.)
   *
   * @param buffer where we write
   */
  public void serialize(ByteBuffer buffer);
}
