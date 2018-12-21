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

package org.apache.druid.query.aggregation.cardinality.accurate.bitmap64;

import org.roaringbitmap.longlong.LongIterator;

/**
 * This class is meant to represent a simple wrapper around an immutable bitmap
 * class.
 */
public interface ImmutableBitmap
{
  /**
   * @return an iterator over the set bits of this bitmap
   */
  LongIterator iterator();

  /**
   * @return The number of bits set to true in this bitmap
   */
  long size();

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
  boolean get(long value);
}
