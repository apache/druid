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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.List;
import java.util.Objects;

/**
 * NOTE: 
 * This class should be deprecated after Druid supports configurable index types on dimensions.
 * When that exists, this should be the implementation: https://github.com/druid-io/druid/issues/2622
 * 
 * This is a stop-gap solution to consolidate the dimension specs and remove the separate spatial 
 * section in DimensionsSpec.
 */
public class NewSpatialDimensionSchema extends DimensionSchema
{
  public static final String DEFAULT_DELIMITER = ",";
  private final List<String> dims;
  private final String delimiter;
  @JsonIgnore
  private final Joiner joiner;
  @JsonIgnore
  private final Splitter splitter;

  @JsonCreator
  public NewSpatialDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("dims") List<String> dims,
      @JsonProperty("delimiter") String delimiter
  )
  {
    super(name, null);
    this.dims = dims;
    this.delimiter = delimiter == null ? DEFAULT_DELIMITER : delimiter;
    this.joiner = Joiner.on(this.delimiter);
    this.splitter = Splitter.on(this.delimiter);
  }

  @JsonProperty
  public List<String> getDims()
  {
    return dims;
  }

  @JsonProperty
  public String getDelimiter()
  {
    return delimiter;
  }

  @Override
  public String getTypeName()
  {
    return DimensionSchema.SPATIAL_TYPE_NAME;
  }

  public Joiner getJoiner()
  {
    return joiner;
  }

  public Splitter getSplitter()
  {
    return splitter;
  }

  @Override
  @JsonIgnore
  public ValueType getValueType()
  {
    return ValueType.STRING;
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

    NewSpatialDimensionSchema that = (NewSpatialDimensionSchema) o;

    if (Objects.equals(dims, that.dims)) {
      return true;
    }

    return this.delimiter.equals(that.delimiter);

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dims != null ? dims.hashCode() : 0);
    result = 31 * result + delimiter.hashCode();
    return result;
  }
}
