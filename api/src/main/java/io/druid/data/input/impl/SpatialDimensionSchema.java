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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.List;
import java.util.Objects;

/**
 */
@Deprecated
public class SpatialDimensionSchema
{
  private final String dimName;
  private final List<String> dims;
  private final String delimiter;
  @JsonIgnore
  private final Joiner joiner;
  @JsonIgnore
  private final Splitter splitter;

  @JsonCreator
  public SpatialDimensionSchema(
      @JsonProperty("dimName") String dimName,
      @JsonProperty("dims") List<String> dims,
      @JsonProperty("delimiter") String delimiter
  )
  {
    this.dimName = dimName;
    this.dims = dims;
    this.delimiter = delimiter == null ? NewSpatialDimensionSchema.DEFAULT_DELIMITER : delimiter;
    this.joiner = Joiner.on(this.delimiter);
    this.splitter = Splitter.on(this.delimiter);
  }

  @JsonProperty
  public String getDimName()
  {
    return dimName;
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

  public Joiner getJoiner()
  {
    return joiner;
  }

  public Splitter getSplitter()
  {
    return splitter;
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

    SpatialDimensionSchema that = (SpatialDimensionSchema) o;

    if (!Objects.equals(dimName, that.dimName)) {
      return false;
    }

    if (!Objects.equals(dims, that.dims)) {
      return false;
    }

    return this.delimiter.equals(that.delimiter);
  }

  @Override
  public int hashCode()
  {
    int result = dimName != null ? dimName.hashCode() : 0;
    result = 31 * result + (dims != null ? dims.hashCode() : 0);
    result = 31 * result + delimiter.hashCode();
    return result;
  }
}
