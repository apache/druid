/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
*/
public class ColumnAnalysis
{
  private static final String ERROR_PREFIX = "error:";

  public static ColumnAnalysis error(String reason)
  {
    return new ColumnAnalysis("STRING", -1, null, ERROR_PREFIX + reason);
  }

  private final String type;
  private final long size;
  private final Integer cardinality;
  private final String errorMessage;

  @JsonCreator
  public ColumnAnalysis(
      @JsonProperty("type") String type,
      @JsonProperty("size") long size,
      @JsonProperty("cardinality") Integer cardinality,
      @JsonProperty("errorMessage") String errorMessage
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
