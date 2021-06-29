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

package org.apache.druid.segment;

import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Interface containing dictionary-related methods common to {@link DimensionSelector},
 * {@link org.apache.druid.segment.vector.SingleValueDimensionVectorSelector}, and
 * {@link org.apache.druid.segment.vector.MultiValueDimensionVectorSelector}.
 */
public interface DimensionDictionarySelector
{
  int CARDINALITY_UNKNOWN = -1;

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * Cardinality may be unknown (e.g. the selector used by IncrementalIndex while reading input rows),
   * in which case this method will return -1. If cardinality is unknown, you should assume this
   * dimension selector has no dictionary, and avoid storing ids, calling "lookupId", or calling "lookupName"
   * outside of the context of operating on a single row.
   *
   * If cardinality is known then it is assumed that underlying dictionary is lexicographically sorted by the encoded
   * value.
   * For example if there are values "A" , "B" , "C" in a column with cardinality 3 then it is assumed that
   * id("A") < id("B") < id("C")
   *
   * @return the value cardinality, or {@link DimensionDictionarySelector#CARDINALITY_UNKNOWN} if unknown.
   */
  int getValueCardinality();

  /**
   * Returns the value for a particular dictionary id as a Java String.
   *
   * For example, if a column has four rows:
   *
   * A,B
   * A
   * A,B
   * B
   *
   * getRow() would return
   *
   * getRow(0) =&gt; [0 1]
   * getRow(1) =&gt; [0]
   * getRow(2) =&gt; [0 1]
   * getRow(3) =&gt; [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) =&gt; A
   * lookupName(1) =&gt; B
   *
   * Performance note: if you want a {@code java.lang.String}, always use this method. It will be at least as fast
   * as calling {@link #lookupNameUtf8} and decoding the bytes. However, if you want UTF-8 bytes, then check if
   * {@link #supportsLookupNameUtf8()} returns true, and if it does, use {@link #lookupNameUtf8} instead.
   *
   * @param id id to lookup the dictionary value for
   *
   * @return dictionary value for the given id, or null if the value is itself null
   */
  @CalledFromHotLoop
  @Nullable
  String lookupName(int id);

  /**
   * Returns the value for a particular dictionary id as UTF-8 bytes.
   *
   * The returned buffer is in big-endian order. It is not reused, so callers may modify the position, limit, byte
   * order, etc of the buffer.
   *
   * The returned buffer may point to the original data, so callers must take care not to use it outside the valid
   * lifetime of this selector. In particular, if the original data came from a reference-counted segment, callers must
   * not use the returned ByteBuffer after releasing their reference to the relevant {@link ReferenceCountingSegment}.
   *
   * Performance note: if you want UTF-8 bytes, and {@link #supportsLookupNameUtf8()} returns true, always use this
   * method. It will be at least as fast as calling {@link #lookupName} and encoding the bytes. However, if you want a
   * {@code java.lang.String}, then use {@link #lookupName} instead of this method.
   *
   * @param id id to lookup the dictionary value for
   *
   * @return dictionary value for the given id, or null if the value is itself null
   *
   * @throws UnsupportedOperationException if {@link #supportsLookupNameUtf8()} is false
   */
  @Nullable
  default ByteBuffer lookupNameUtf8(int id)
  {
    // If UTF-8 isn't faster, it's better to throw an exception rather than delegate to "lookupName" and do the
    // conversion. Callers should check "supportsLookupNameUtf8" to make sure they're calling the fastest method.
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether this selector supports {@link #lookupNameUtf8}.
   */
  default boolean supportsLookupNameUtf8()
  {
    return false;
  }

  /**
   * Returns true if it is possible to {@link #lookupName(int)} by ids from 0 to {@link #getValueCardinality()}
   * before the rows with those ids are returned.
   *
   * <p>Returns false if {@link #lookupName(int)} could be called with ids, returned from the most recent row (or row
   * vector) returned by this DimensionSelector, but not earlier. If {@link #getValueCardinality()} of this
   * selector additionally returns {@link #CARDINALITY_UNKNOWN}, {@code lookupName()} couldn't be called with
   * ids, returned by not the most recent row (or row vector), i. e. names for ids couldn't be looked up "later". If
   * {@link #getValueCardinality()} returns a non-negative number, {@code lookupName()} could be called with any ids,
   * returned from rows (or row vectors) returned since the creation of this DimensionSelector.
   *
   * <p>If {@link #lookupName(int)} is called with an ineligible id, result is undefined: exception could be thrown, or
   * null returned, or some other random value.
   */
  boolean nameLookupPossibleInAdvance();

  /**
   * Returns {@link IdLookup} if available for this DimensionSelector, or null.
   */
  @Nullable
  IdLookup idLookup();
}
