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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * This interface is used to expose information about columns that is interesting to know for all matters dealing with
 * reading from columns, including query planning and optimization, creating readers to merge segments at ingestion
 * time, and probably nearly anything else you can imagine.
 */
public interface ColumnCapabilities
{
  /**
   * Column type, good to know so caller can know what to expect and which optimal selector to use
   */
  ValueType getType();

  /**
   *
   * If ValueType is COMPLEX, then the typeName associated with it.
   */
  String getComplexTypeName();

  /**
   * Is the column dictionary encoded? If so, a DimensionDictionarySelector may be used instead of using a value
   * selector, allowing algorithms to operate on primitive integer dictionary ids rather than the looked up dictionary
   * values
   */
  Capable isDictionaryEncoded();

  /**
   * If the column is dictionary encoded, are those values sorted? Useful to know for optimizations that can defer
   * looking up values and allowing sorting with the dictionary ids directly
   */

  Capable areDictionaryValuesSorted();

  /**
   * If the column is dictionary encoded, is there a 1:1 mapping of dictionary ids to values? If this is true, it
   * unlocks optimizations such as allowing for things like grouping directly on dictionary ids and deferred value
   * lookup
   */
  Capable areDictionaryValuesUnique();

  /**
   * String columns are sneaky, and might have multiple values, this is to allow callers to know and appropriately
   * prepare themselves
   */
  Capable hasMultipleValues();

  /**
   * Does the column have an inverted index bitmap for each value? If so, these may be employed to 'pre-filter' the
   * column by examining if the values match the filter and intersecting the bitmaps, to avoid having to scan and
   * evaluate if every row matches the filter
   */
  boolean hasBitmapIndexes();

  /**
   * Does the column have spatial indexes available to allow use with spatial filtering?
   */
  boolean hasSpatialIndexes();

  /**
   * All Druid primitive columns support filtering, maybe with or without indexes, but by default complex columns
   * do not support direct filtering, unless provided by through a custom implementation.
   */
  boolean isFilterable();

  /**
   * Does this column contain null values? If so, callers, especially for primitive numeric columns, will need to check
   * for null value rows and act accordingly
   */
  Capable hasNulls();

  enum Capable
  {
    FALSE,
    TRUE,
    UNKNOWN;

    public boolean isTrue()
    {
      return this == TRUE;
    }

    public boolean isMaybeTrue()
    {
      return isTrue() || isUnknown();
    }

    public boolean isFalse()
    {
      return this == FALSE;
    }

    public boolean isUnknown()
    {
      return this == UNKNOWN;
    }

    public Capable coerceUnknownToBoolean(boolean unknownIsTrue)
    {
      return this == UNKNOWN ? Capable.of(unknownIsTrue) : this;
    }

    public Capable and(Capable other)
    {
      if (this == UNKNOWN || other == UNKNOWN) {
        return UNKNOWN;
      }
      return this == TRUE && other == TRUE ? TRUE : FALSE;
    }

    public Capable or(Capable other)
    {
      if (this == TRUE) {
        return TRUE;
      }
      return other;
    }

    public static Capable of(boolean bool)
    {
      return bool ? TRUE : FALSE;
    }

    @JsonCreator
    public static Capable ofNullable(@Nullable Boolean bool)
    {
      return bool == null ? Capable.UNKNOWN : of(bool);
    }

    @JsonValue
    @Nullable
    public Boolean toJson()
    {
      return this == UNKNOWN ? null : isTrue();
    }

    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(super.toString());
    }
  }

  /**
   * This interface define the shape of a mechnism to allow for bespoke coercion of {@link Capable#UNKNOWN} into
   * {@link Capable#TRUE} or {@link Capable#FALSE} for each {@link Capable} of a {@link ColumnCapabilities}, as is
   * appropriate for the situation of the caller.
   */
  interface CoercionLogic
  {
    /**
     * If {@link ColumnCapabilities#isDictionaryEncoded()} is {@link Capable#UNKNOWN}, define if it should be treated
     * as true or false.
     */
    boolean dictionaryEncoded();

    /**
     * If {@link ColumnCapabilities#areDictionaryValuesSorted()} ()} is {@link Capable#UNKNOWN}, define if it should be
     * treated as true or false.
     */
    boolean dictionaryValuesSorted();

    /**
     * If {@link ColumnCapabilities#areDictionaryValuesUnique()} ()} is {@link Capable#UNKNOWN}, define if it should be
     * treated as true or false.
     */
    boolean dictionaryValuesUnique();

    /**
     * If {@link ColumnCapabilities#hasMultipleValues()} is {@link Capable#UNKNOWN}, define if it should be treated
     * as true or false.
     */
    boolean multipleValues();

    /**
     * If {@link ColumnCapabilities#hasNulls()} is {@link Capable#UNKNOWN}, define if it should be treated as true
     * or false
     */
    boolean hasNulls();
  }
}
