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

package io.druid.segment;

import com.google.common.base.Predicate;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.HotLoopCallee;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

/**
 */
public interface DimensionSelector extends ColumnValueSelector, HotLoopCallee
{
  public static int CARDINALITY_UNKNOWN = -1;

  /**
   * Gets all values for the row inside of an IntBuffer.  I.e. one possible implementation could be
   *
   * return IntBuffer.wrap(lookupExpansion(get());
   *
   * @return all values for the row as an IntBuffer
   */
  @CalledFromHotLoop
  public IndexedInts getRow();

  /**
   * @param value nullable dimension value
   */
  ValueMatcher makeValueMatcher(String value);

  ValueMatcher makeValueMatcher(Predicate<String> predicate);

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
   * @return the value cardinality, or -1 if unknown.
   */
  public int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
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
   * @param id id to lookup the field name for
   * @return the field name for the given id
   */
  @CalledFromHotLoop
  public String lookupName(int id);

  /**
   * Returns true if it is possible to {@link #lookupName(int)} by ids from 0 to {@link #getValueCardinality()}
   * before the rows with those ids are returned.
   *
   * <p>Returns false if {@link #lookupName(int)} could be called with ids, returned from the most recent call of {@link
   * #getRow()} on this DimensionSelector, but not earlier. If {@link #getValueCardinality()} of this DimensionSelector
   * additionally returns {@link #CARDINALITY_UNKNOWN}, {@code lookupName()} couldn't be called with ids, returned by
   * not the most recent call of {@link #getRow()}, i. e. names for ids couldn't be looked up "later". If {@link
   * #getValueCardinality()} returns a non-negative number, {@code lookupName()} could be called with any ids, returned
   * from {@code #getRow()} since the creation of this DimensionSelector.
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
