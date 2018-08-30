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

package org.apache.druid.collections.bitmap;

/**
 * This class is meant to represent a simple wrapper around a bitmap class.
 */
public interface MutableBitmap extends ImmutableBitmap
{
  /**
   * Empties the content of this bitmap.
   */
  void clear();

  /**
   * Compute the bitwise-or of this bitmap with another bitmap. The current
   * bitmap is modified whereas the other bitmap is left intact.
   *
   * Note that the other bitmap should be of the same class instance.
   *
   * @param mutableBitmap other bitmap
   */
  void or(MutableBitmap mutableBitmap);


  /**
   * Return the size in bytes for the purpose of serialization to a ByteBuffer.
   * Note that this is distinct from the memory usage.
   *
   * @return the total set in bytes
   */
  int getSizeInBytes();

  /**
   * Add the specified integer to the bitmap. This is equivalent to setting the
   * ith bit to the value 1.
   *
   * @param entry integer to be added
   */
  void add(int entry);

  /**
   * Remove the specified integer to the bitmap. This is equivalent to setting the
   * ith bit to the value 1.
   *
   * @param entry integer to be remove
   */
  void remove(int entry);

}
