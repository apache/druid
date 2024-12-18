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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 */
public class ColumnAnalysis
{
  private static final String ERROR_PREFIX = "error:";

  public static <T> Builder<T> builder()
  {
    return new Builder<>();
  }

  public static ColumnAnalysis error(String reason)
  {
    return builder().withType(ColumnType.STRING)
                    .withSize(-1)
                    .withErrorMessage(reason)
                    .build();
  }

  private final String type;
  private final ColumnType typeSignature;
  private final boolean hasMultipleValues;
  private final boolean hasNulls;
  private final long size;
  private final Integer cardinality;
  private final Comparable minValue;
  private final Comparable maxValue;
  private final String errorMessage;

  @JsonCreator
  public ColumnAnalysis(
      @JsonProperty("typeSignature") ColumnType typeSignature,
      @JsonProperty("type") String type,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("hasNulls") boolean hasNulls,
      @JsonProperty("size") long size,
      @JsonProperty("cardinality") Integer cardinality,
      @JsonProperty("minValue") Comparable minValue,
      @JsonProperty("maxValue") Comparable maxValue,
      @JsonProperty("errorMessage") String errorMessage
  )
  {
    this.typeSignature = typeSignature;
    this.type = type;
    this.hasMultipleValues = hasMultipleValues;
    this.hasNulls = hasNulls;
    this.size = size;
    this.cardinality = cardinality;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.errorMessage = errorMessage;
  }

  @JsonProperty
  public ColumnType getTypeSignature()
  {
    return typeSignature;
  }

  @JsonProperty
  @Deprecated
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

  @JsonProperty
  public boolean isHasNulls()
  {
    return hasNulls;
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

    if (!Objects.equals(type, rhs.getType())) {
      return ColumnAnalysis.error(
          StringUtils.format("cannot_merge_diff_types: [%s] and [%s]", type, rhs.getType())
      );
    }

    if (!Objects.equals(typeSignature, rhs.getTypeSignature())) {
      return ColumnAnalysis.error(
          StringUtils.format(
              "cannot_merge_diff_types: [%s] and [%s]",
              typeSignature.asTypeString(),
              rhs.getTypeSignature().asTypeString()
          )
      );
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

    // min and max are currently set for only string columns
    if (typeSignature.equals(ColumnType.STRING)) {
      newMin = NullHandling.nullToEmptyIfNeeded((String) newMin);
      newMax = NullHandling.nullToEmptyIfNeeded((String) newMax);
    }
    return builder().withType(typeSignature)
                    .withTypeName(type)
                    .hasMultipleValues(multipleValues)
                    .hasNulls(hasNulls || rhs.hasNulls)
                    .withSize(size + rhs.getSize())
                    .withCardinality(cardinality)
                    .withMinValue(newMin)
                    .withMaxValue(newMax)
                    .build();
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
           "typeSignature='" + typeSignature + '\'' +
           ", type=" + type +
           ", hasMultipleValues=" + hasMultipleValues +
           ", hasNulls=" + hasNulls +
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
           hasNulls == that.hasNulls &&
           size == that.size &&
           Objects.equals(typeSignature, that.typeSignature) &&
           Objects.equals(type, that.type) &&
           Objects.equals(cardinality, that.cardinality) &&
           Objects.equals(minValue, that.minValue) &&
           Objects.equals(maxValue, that.maxValue) &&
           Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        typeSignature,
        type,
        hasMultipleValues,
        hasNulls,
        size,
        cardinality,
        minValue,
        maxValue,
        errorMessage
    );
  }

  public static class Builder<T>
  {
    private ColumnType typeSignature;
    private String typeName;
    private boolean hasMultipleValues;
    private boolean hasNulls;
    private long size;
    private Integer cardinality;
    private Comparable<T> minValue;
    private Comparable<T> maxValue;
    private String errorMessage;

    public Builder withCapabilities(ColumnCapabilities capabilities)
    {

      return withType(capabilities.toColumnType()).hasMultipleValues(capabilities.hasMultipleValues().isTrue())
                                                  // if we don't know for sure, then we should plan to check for nulls
                                                  .hasNulls(capabilities.hasNulls().isMaybeTrue());
    }

    public Builder withType(ColumnType columnType)
    {
      this.typeSignature = columnType;
      return this;
    }

    public Builder withTypeName(String typeName)
    {
      this.typeName = typeName;
      return this;
    }

    public Builder hasMultipleValues(boolean hasMultipleValues)
    {
      this.hasMultipleValues = hasMultipleValues;
      return this;
    }

    public Builder hasNulls(boolean hasNulls)
    {
      this.hasNulls = hasNulls;
      return this;
    }

    public Builder withSize(long size)
    {
      this.size = size;
      return this;
    }

    public Builder withCardinality(@Nullable Integer cardinality)
    {
      this.cardinality = cardinality;
      return this;
    }

    public Builder withMinValue(Comparable<T> minValue)
    {
      this.minValue = minValue;
      return this;
    }

    public Builder withMaxValue(Comparable<T> maxValue)
    {
      this.maxValue = maxValue;
      return this;
    }

    public Builder withErrorMessage(String errorMessage)
    {
      this.errorMessage = ERROR_PREFIX + errorMessage;
      return this;
    }

    public ColumnAnalysis build()
    {
      return new ColumnAnalysis(
          typeSignature,
          typeName == null ? typeSignature.getType().name() : typeName,
          hasMultipleValues,
          hasNulls,
          size,
          cardinality,
          minValue,
          maxValue,
          errorMessage
      );
    }
  }
}
