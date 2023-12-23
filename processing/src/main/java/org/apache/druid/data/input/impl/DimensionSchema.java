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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;

import java.util.Objects;

/**
 */
@PublicApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringDimensionSchema.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = DimensionSchema.STRING_TYPE_NAME, value = StringDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.LONG_TYPE_NAME, value = LongDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.FLOAT_TYPE_NAME, value = FloatDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.DOUBLE_TYPE_NAME, value = DoubleDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.SPATIAL_TYPE_NAME, value = NewSpatialDimensionSchema.class),
    @JsonSubTypes.Type(name = NestedDataComplexTypeSerde.TYPE_NAME, value = NestedDataColumnSchema.class),
    @JsonSubTypes.Type(name = AutoTypeColumnSchema.TYPE, value = AutoTypeColumnSchema.class)
})
public abstract class DimensionSchema
{
  public static DimensionSchema getDefaultSchemaForBuiltInType(String name, TypeSignature<ValueType> type)
  {
    switch (type.getType()) {
      case STRING:
        return new StringDimensionSchema(name);
      case LONG:
        return new LongDimensionSchema(name);
      case FLOAT:
        return new FloatDimensionSchema(name);
      case DOUBLE:
        return new DoubleDimensionSchema(name);
      default:
        // the auto column indexer can handle any type
        return new AutoTypeColumnSchema(name, null);
    }
  }

  public static final String STRING_TYPE_NAME = "string";
  public static final String LONG_TYPE_NAME = "long";
  public static final String FLOAT_TYPE_NAME = "float";
  public static final String SPATIAL_TYPE_NAME = "spatial";
  public static final String DOUBLE_TYPE_NAME = "double";
  private static final EmittingLogger log = new EmittingLogger(DimensionSchema.class);

  public enum MultiValueHandling
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
  private final boolean createBitmapIndex;

  protected DimensionSchema(String name, MultiValueHandling multiValueHandling, boolean createBitmapIndex)
  {
    if (Strings.isNullOrEmpty(name)) {
      log.warn("Null or Empty Dimension found");
    }
    this.name = name;
    this.multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;
    this.createBitmapIndex = createBitmapIndex;
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

  @JsonProperty("createBitmapIndex")
  public boolean hasBitmapIndex()
  {
    return createBitmapIndex;
  }

  @JsonIgnore
  public abstract String getTypeName();

  @JsonIgnore
  public abstract ColumnType getColumnType();

  @JsonIgnore
  public DimensionHandler getDimensionHandler()
  {
    // default implementation for backwards compatibility
    return DimensionHandlerUtils.getHandlerFromCapabilities(
        name,
        IncrementalIndex.makeDefaultCapabilitiesFromValueType(getColumnType()),
        multiValueHandling
    );
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DimensionSchema that = (DimensionSchema) o;
    return createBitmapIndex == that.createBitmapIndex &&
           Objects.equals(name, that.name) &&
           Objects.equals(getTypeName(), that.getTypeName()) &&
           Objects.equals(getColumnType(), that.getColumnType()) &&
           multiValueHandling == that.multiValueHandling;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, multiValueHandling, createBitmapIndex, getTypeName(), getColumnType());
  }

  @Override
  public String toString()
  {
    return "DimensionSchema{" +
           "name='" + name + '\'' +
           ", valueType=" + getColumnType() +
           ", typeName=" + getTypeName() +
           ", multiValueHandling=" + multiValueHandling +
           ", createBitmapIndex=" + createBitmapIndex +
           '}';
  }
}
