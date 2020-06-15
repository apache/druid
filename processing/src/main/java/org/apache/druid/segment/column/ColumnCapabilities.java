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
 */
public interface ColumnCapabilities
{
  ValueType getType();
  boolean isDictionaryEncoded();
  Capable areDictionaryValuesSorted();
  Capable areDictionaryValuesUnique();
  boolean isRunLengthEncoded();
  boolean hasBitmapIndexes();
  boolean hasSpatialIndexes();
  Capable hasMultipleValues();
  boolean isFilterable();

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
}
