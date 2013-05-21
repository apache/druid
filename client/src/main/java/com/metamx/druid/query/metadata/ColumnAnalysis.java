/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.query.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.index.column.ValueType;

/**
*/
public class ColumnAnalysis
{
  private static final String ERROR_PREFIX = "error:";

  public static ColumnAnalysis error(String reason)
  {
    return new ColumnAnalysis(ValueType.STRING, -1, null, ERROR_PREFIX + reason);
  }

  private final String type;
  private final long size;
  private final Integer cardinality;
  private final String errorMessage;

  @JsonCreator
  public ColumnAnalysis(
      @JsonProperty("type") ValueType type,
      @JsonProperty("size") long size,
      @JsonProperty("cardinality") Integer cardinality,
      @JsonProperty("errorMessage") String errorMessage
  )
  {
    this(type.name(), size, cardinality, errorMessage);
  }

  private ColumnAnalysis(
      String type,
      long size,
      Integer cardinality,
      String errorMessage
  )
  {
    this.type = type;
    this.size = size;
    this.cardinality = cardinality;
    this.errorMessage = errorMessage;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  public Integer getCardinality()
  {
    return cardinality;
  }

  @JsonProperty
  public String getErrorMessage()
  {
    return errorMessage;
  }

  public boolean isError()
  {
    return (errorMessage != null && !errorMessage.isEmpty());
  }

  public ColumnAnalysis fold(ColumnAnalysis rhs)
  {
    if (rhs == null) {
      return this;
    }

    if (!type.equals(rhs.getType())) {
      return ColumnAnalysis.error("cannot_merge_diff_types");
    }

    Integer cardinality = getCardinality();
    final Integer rhsCardinality = rhs.getCardinality();
    if (cardinality == null) {
      cardinality = rhsCardinality;
    }
    else {
      if (rhsCardinality != null) {
        cardinality = Math.max(cardinality, rhsCardinality);
      }
    }

    return new ColumnAnalysis(type, size + rhs.getSize(), cardinality, null);
  }

  @Override
  public String toString()
  {
    return "ColumnAnalysis{" +
           "type='" + type + '\'' +
           ", size=" + size +
           ", cardinality=" + cardinality +
           ", errorMessage='" + errorMessage + '\'' +
           '}';
  }
}
