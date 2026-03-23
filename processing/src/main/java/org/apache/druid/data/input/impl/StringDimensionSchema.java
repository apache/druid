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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.StringDimensionHandler;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public class StringDimensionSchema extends DimensionSchema
{
  private static final boolean DEFAULT_CREATE_BITMAP_INDEX = true;

  @Nullable
  public static Integer getDefaultMaxStringLength()
  {
    return BuiltInTypesModule.getMaxStringLength();
  }

  @Nullable
  private final Integer maxStringLength;

  @JsonCreator
  public static StringDimensionSchema create(String name)
  {
    return new StringDimensionSchema(name);
  }

  @JsonCreator
  public StringDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("multiValueHandling") MultiValueHandling multiValueHandling,
      @JsonProperty("createBitmapIndex") Boolean createBitmapIndex,
      @JsonProperty("maxStringLength") @Nullable Integer maxStringLength
  )
  {
    super(name, multiValueHandling, createBitmapIndex == null ? DEFAULT_CREATE_BITMAP_INDEX : createBitmapIndex);
    this.maxStringLength = maxStringLength != null ? maxStringLength : getDefaultMaxStringLength();
  }

  public StringDimensionSchema(
      String name,
      MultiValueHandling multiValueHandling,
      Boolean createBitmapIndex
  )
  {
    this(name, multiValueHandling, createBitmapIndex, null);
  }

  public StringDimensionSchema(String name)
  {
    this(name, null, DEFAULT_CREATE_BITMAP_INDEX, null);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Integer getMaxStringLength()
  {
    return maxStringLength;
  }

  @Override
  public String getTypeName()
  {
    return DimensionSchema.STRING_TYPE_NAME;
  }

  @Override
  @JsonIgnore
  public ColumnType getColumnType()
  {
    return ColumnType.STRING;
  }

  @Override
  public boolean canBeMultiValued()
  {
    return true;
  }

  @Override
  public DimensionHandler getDimensionHandler()
  {
    return new StringDimensionHandler(getName(), getMultiValueHandling(), hasBitmapIndex(), false, maxStringLength);
  }
}
