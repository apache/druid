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

import java.nio.ByteBuffer;

public interface BitmapFactory
{
  /**
   * Create a new empty bitmap
   *
   * @return the new bitmap
   */
  MutableBitmap makeEmptyMutableBitmap();

  ImmutableBitmap makeEmptyImmutableBitmap();

  ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap);

  /**
   * Given a ByteBuffer pointing at a serialized version of a bitmap,
   * instantiate an immutable mapped bitmap.
   * 
   * When using RoaringBitmap (with the RoaringBitmapFactory class), it is not
   * necessary for b.limit() to indicate the end of the serialized content
   * whereas it is critical to set b.limit() appropriately with ConciseSet (with
   * the ConciseBitmapFactory).
   *
   * @param b the input byte buffer
   *
   * @return the new bitmap
   */
  ImmutableBitmap mapImmutableBitmap(ByteBuffer b);

  /**
   * Compute the union (bitwise-OR) of a set of bitmaps. They are assumed to be
   * instances of of the proper WrappedConciseBitmap otherwise a ClassCastException
   * is thrown.
   *
   * @param b input ImmutableGenericBitmap objects
   *
   * @return the union.
   *
   * @throws ClassCastException if one of the ImmutableGenericBitmap objects if not an instance
   *                            of WrappedImmutableConciseBitmap
   */
  ImmutableBitmap union(Iterable<ImmutableBitmap> b);

  /**
   * Compute the intersection (bitwise-AND) of a set of bitmaps. They are assumed to be
   * instances of of the proper WrappedConciseBitmap otherwise a ClassCastException
   * is thrown.
   *
   * @param b input ImmutableGenericBitmap objects
   *
   * @return the union.
   *
   * @throws ClassCastException if one of the ImmutableGenericBitmap objects if not an instance
   *                            of WrappedImmutableConciseBitmap
   */
  ImmutableBitmap intersection(Iterable<ImmutableBitmap> b);

  ImmutableBitmap complement(ImmutableBitmap b, int length);
}
