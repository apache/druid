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

package org.apache.druid.sql.calcite.filtration;

import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Objects;

public class RangeRefKey
{
  private final String column;
  private final ColumnType matchValueType;
  @Nullable
  private final ExtractionFn extractionFn;

  public RangeRefKey(String column, ColumnType matchValueType, ExtractionFn extractionFn)
  {
    this.column = column;
    this.matchValueType = matchValueType;
    this.extractionFn = extractionFn;
  }

  public static RangeRefKey from(RangeFilter filter)
  {
    return new RangeRefKey(
        filter.getColumn(),
        filter.getMatchValueType(),
        filter.getExtractionFn()
    );
  }

  public static RangeRefKey from(EqualityFilter filter)
  {
    return new RangeRefKey(
        filter.getColumn(),
        filter.getMatchValueType(),
        filter.getExtractionFn()
    );
  }

  public String getColumn()
  {
    return column;
  }

  public ColumnType getMatchValueType()
  {
    return matchValueType;
  }

  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RangeRefKey that = (RangeRefKey) o;
    return Objects.equals(column, that.column)
           && Objects.equals(matchValueType, that.matchValueType)
           && Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, matchValueType, extractionFn);
  }

  @Override
  public String toString()
  {
    return "RangeRefKey{" +
           "column='" + column + '\'' +
           ", matchValueType=" + matchValueType +
           ", extractionFn=" + extractionFn +
           '}';
  }
}
