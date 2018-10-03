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

package org.apache.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

/**
 */
public class ColumnAnalysis
{
  private static final String ERROR_PREFIX = "error:";

  public static ColumnAnalysis error(String reason)
  {
    return new ColumnAnalysis("STRING", false, -1, null, null, null, ERROR_PREFIX + reason);
  }

  private final String type;
  private final boolean hasMultipleValues;
  private final long size;
  private final Integer cardinality;
  private final Comparable minValue;
  private final Comparable maxValue;
  private final String errorMessage;

  @JsonCreator
  public ColumnAnalysis(
      @JsonProperty("type") String type,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("size") long size,
      @JsonProperty("cardinality") Integer cardinality,
      @JsonProperty("minValue") Comparable minValue,
      @JsonProperty("maxValue") Comparable maxValue,
      @JsonProperty("errorMessage") String errorMessage
  )
  {
    this.type = type;
    this.hasMultipleValues = hasMultipleValues;
    this.size = size;
    this.cardinality = cardinality;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.errorMessage = errorMessage;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
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

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
  @JsonProperty
  public Comparable getMinValue()
  {
    return minValue;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
  @JsonProperty
  public Comparable getMaxValue()
  {
    return maxValue;
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

    if (isError() && rhs.isError()) {
      return errorMessage.equals(rhs.getErrorMessage()) ? this : ColumnAnalysis.error("multiple_errors");
    } else if (isError()) {
      return this;
    } else if (rhs.isError()) {
      return rhs;
    }

    if (!type.equals(rhs.getType())) {
      return ColumnAnalysis.error("cannot_merge_diff_types");
    }

    Integer cardinality = getCardinality();
    final Integer rhsCardinality = rhs.getCardinality();
    if (cardinality == null) {
      cardinality = rhsCardinality;
    } else if (rhsCardinality != null) {
      cardinality = Math.max(cardinality, rhsCardinality);
    }

    final boolean multipleValues = hasMultipleValues || rhs.isHasMultipleValues();

    Comparable newMin = choose(minValue, rhs.minValue, false);
    Comparable newMax = choose(maxValue, rhs.maxValue, true);

    return new ColumnAnalysis(type, multipleValues, size + rhs.getSize(), cardinality, newMin, newMax, null);
  }

  private <T extends Comparable> T choose(T obj1, T obj2, boolean max)
  {
    if (obj1 == null) {
      return max ? obj2 : null;
    }
    if (obj2 == null) {
      return max ? obj1 : null;
    }
    int compare = max ? obj1.compareTo(obj2) : obj2.compareTo(obj1);
    return compare > 0 ? obj1 : obj2;
  }

  @Override
  public String toString()
  {
    return "ColumnAnalysis{" +
           "type='" + type + '\'' +
           ", hasMultipleValues=" + hasMultipleValues +
           ", size=" + size +
           ", cardinality=" + cardinality +
           ", minValue=" + minValue +
           ", maxValue=" + maxValue +
           ", errorMessage='" + errorMessage + '\'' +
           '}';
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
    ColumnAnalysis that = (ColumnAnalysis) o;
    return hasMultipleValues == that.hasMultipleValues &&
           size == that.size &&
           Objects.equals(type, that.type) &&
           Objects.equals(cardinality, that.cardinality) &&
           Objects.equals(minValue, that.minValue) &&
           Objects.equals(maxValue, that.maxValue) &&
           Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, hasMultipleValues, size, cardinality, minValue, maxValue, errorMessage);
  }
}
