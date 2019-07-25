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

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.IntIterator;

/**
 * This class is meant to represent a simple wrapper around an immutable bitmap
 * class.
 */
public interface ImmutableBitmap
{
  /**
   * @return an iterator over the set bits of this bitmap
   */
  IntIterator iterator();

  /**
   * @return a batched iterator over the set bits of this bitmap
   */
  default BatchIterator batchIterator()
  {
    return new BatchIteratorAdapter(iterator());
  }

  /**
   * @return The number of bits set to true in this bitmap
   */
  int size();

  byte[] toBytes();

  /**
   * @return True if this bitmap is empty (contains no set bit)
   */
  boolean isEmpty();

  /**
   * Returns true if the bit at position value is set
   *
   * @param value the position to check
   *
   * @return true if bit is set
   */
  boolean get(int value);

  /**
   * Compute the bitwise-and of this bitmap with another bitmap. A new bitmap is generated.
   *
   * Note that the other bitmap should be of the same class instance.
   *
   * @param otherBitmap other bitmap
   */
  ImmutableBitmap intersection(ImmutableBitmap otherBitmap);

}
