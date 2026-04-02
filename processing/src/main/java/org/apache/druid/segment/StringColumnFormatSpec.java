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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionSchema;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringColumnFormatSpec
{
  private static final StringColumnFormatSpec DEFAULT =
      builder()
          .setCreateBitmapIndex(true)
          .setMultiValueHandling(DimensionSchema.MultiValueHandling.SORTED_ARRAY)
          .build();

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(StringColumnFormatSpec spec)
  {
    return new Builder(spec);
  }

  public static StringColumnFormatSpec getEffectiveFormatSpec(
      @Nullable StringColumnFormatSpec columnFormatSpec,
      IndexSpec indexSpec
  )
  {
    final Builder builder = columnFormatSpec == null ? builder() : builder(columnFormatSpec);

    final StringColumnFormatSpec defaultSpec;
    if (indexSpec.getStringColumnFormatSpec() != null) {
      defaultSpec = indexSpec.getStringColumnFormatSpec();
    } else {
      defaultSpec = DEFAULT;
    }

    if (builder.createBitmapIndex == null) {
      if (defaultSpec.getCreateBitmapIndex() != null) {
        builder.setCreateBitmapIndex(defaultSpec.getCreateBitmapIndex());
      } else {
        builder.setCreateBitmapIndex(DEFAULT.getCreateBitmapIndex());
      }
    }

    if (builder.multiValueHandling == null) {
      if (defaultSpec.getMultiValueHandling() != null) {
        builder.setMultiValueHandling(defaultSpec.getMultiValueHandling());
      } else {
        builder.setMultiValueHandling(DEFAULT.getMultiValueHandling());
      }
    }

    if (builder.maxStringLength == null) {
      // No DEFAULT fallback needed: null means "no truncation"
      builder.setMaxStringLength(defaultSpec.getMaxStringLength());
    }

    return builder.build();
  }

  @Nullable
  private final Boolean createBitmapIndex;

  @Nullable
  private final DimensionSchema.MultiValueHandling multiValueHandling;

  @Nullable
  private final Integer maxStringLength;

  @JsonCreator
  public StringColumnFormatSpec(
      @JsonProperty("createBitmapIndex") @Nullable Boolean createBitmapIndex,
      @JsonProperty("multiValueHandling") @Nullable DimensionSchema.MultiValueHandling multiValueHandling,
      @JsonProperty("maxStringLength") @Nullable Integer maxStringLength
  )
  {
    this.createBitmapIndex = createBitmapIndex;
    this.multiValueHandling = multiValueHandling;
    this.maxStringLength = maxStringLength;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean getCreateBitmapIndex()
  {
    return createBitmapIndex;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DimensionSchema.MultiValueHandling getMultiValueHandling()
  {
    return multiValueHandling;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getMaxStringLength()
  {
    return maxStringLength;
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
    StringColumnFormatSpec that = (StringColumnFormatSpec) o;
    return Objects.equals(createBitmapIndex, that.createBitmapIndex)
           && multiValueHandling == that.multiValueHandling
           && Objects.equals(maxStringLength, that.maxStringLength);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(createBitmapIndex, multiValueHandling, maxStringLength);
  }

  @Override
  public String toString()
  {
    return "StringColumnFormatSpec{" +
           "createBitmapIndex=" + createBitmapIndex +
           ", multiValueHandling=" + multiValueHandling +
           ", maxStringLength=" + maxStringLength +
           '}';
  }

  public static class Builder
  {
    @Nullable
    private Boolean createBitmapIndex;
    @Nullable
    private DimensionSchema.MultiValueHandling multiValueHandling;
    @Nullable
    private Integer maxStringLength;

    public Builder()
    {
    }

    public Builder(StringColumnFormatSpec spec)
    {
      this.createBitmapIndex = spec.createBitmapIndex;
      this.multiValueHandling = spec.multiValueHandling;
      this.maxStringLength = spec.maxStringLength;
    }

    public Builder setCreateBitmapIndex(@Nullable Boolean createBitmapIndex)
    {
      this.createBitmapIndex = createBitmapIndex;
      return this;
    }

    public Builder setMultiValueHandling(@Nullable DimensionSchema.MultiValueHandling multiValueHandling)
    {
      this.multiValueHandling = multiValueHandling;
      return this;
    }

    public Builder setMaxStringLength(@Nullable Integer maxStringLength)
    {
      this.maxStringLength = maxStringLength;
      return this;
    }

    public StringColumnFormatSpec build()
    {
      return new StringColumnFormatSpec(createBitmapIndex, multiValueHandling, maxStringLength);
    }
  }
}
