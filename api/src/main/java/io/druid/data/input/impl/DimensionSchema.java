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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.StringUtils;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringDimensionSchema.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = DimensionSchema.STRING_TYPE_NAME, value = StringDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.LONG_TYPE_NAME, value = LongDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.FLOAT_TYPE_NAME, value = FloatDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.DOUBLE_TYPE_NAME, value = DoubleDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.SPATIAL_TYPE_NAME, value = NewSpatialDimensionSchema.class),
})
public abstract class DimensionSchema
{
  public static final String STRING_TYPE_NAME = "string";
  public static final String LONG_TYPE_NAME = "long";
  public static final String FLOAT_TYPE_NAME = "float";
  public static final String SPATIAL_TYPE_NAME = "spatial";
  public static final String DOUBLE_TYPE_NAME = "double";


  // main druid and druid-api should really use the same ValueType enum.
  // merge them when druid-api is merged back into the main repo
  public enum ValueType
  {
    FLOAT,
    LONG,
    STRING,
    DOUBLE,
    COMPLEX;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toUpperCase(this.name());
    }

    @JsonCreator
    public static ValueType fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  public static enum MultiValueHandling
  {
    SORTED_ARRAY,
    SORTED_SET,
    ARRAY {
      @Override
      public boolean needSorting()
      {
        return false;
      }
    };

    public boolean needSorting()
    {
      return true;
    }

    @Override
    @JsonValue
    public String toString()
    {
      return StringUtils.toUpperCase(name());
    }

    @JsonCreator
    public static MultiValueHandling fromString(String name)
    {
      return name == null ? ofDefault() : valueOf(StringUtils.toUpperCase(name));
    }

    // this can be system configuration
    public static MultiValueHandling ofDefault()
    {
      return SORTED_ARRAY;
    }
  }

  private final String name;
  private final MultiValueHandling multiValueHandling;

  protected DimensionSchema(String name, MultiValueHandling multiValueHandling)
  {
    this.name = Preconditions.checkNotNull(name, "Dimension name cannot be null.");
    this.multiValueHandling = multiValueHandling;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public MultiValueHandling getMultiValueHandling()
  {
    return multiValueHandling;
  }

  @JsonIgnore
  public abstract String getTypeName();

  @JsonIgnore
  public abstract ValueType getValueType();

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionSchema that = (DimensionSchema) o;

    return name.equals(that.name);

  }

  @Override
  public int hashCode()
  {
    return name.hashCode();
  }
}
