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
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.StringColumnFormatSpec;
import org.apache.druid.segment.StringDimensionHandler;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringDimensionSchema extends DimensionSchema
{
  private static final boolean DEFAULT_CREATE_BITMAP_INDEX = true;

  @Nullable
  private final StringColumnFormatSpec columnFormatSpec;

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
      @JsonProperty("columnFormatSpec") @Nullable StringColumnFormatSpec columnFormatSpec
  )
  {
    super(name, multiValueHandling, createBitmapIndex == null ? DEFAULT_CREATE_BITMAP_INDEX : createBitmapIndex);
    this.columnFormatSpec = columnFormatSpec;
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

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public StringColumnFormatSpec getColumnFormatSpec()
  {
    return columnFormatSpec;
  }

  @Override
  public DimensionSchema getEffectiveSchema(IndexSpec indexSpec)
  {
    // If there's no per-column or job-level string format config, nothing to resolve
    if (columnFormatSpec == null && indexSpec.getStringColumnFormatSpec() == null) {
      return this;
    }
    StringColumnFormatSpec effective =
        StringColumnFormatSpec.getEffectiveFormatSpec(columnFormatSpec, indexSpec);
    return new StringDimensionSchema(
        getName(),
        getMultiValueHandling(),
        hasBitmapIndex(),
        effective
    );
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
    MultiValueHandling mvh = getMultiValueHandling();
    boolean bitmap = hasBitmapIndex();
    Integer maxStringLength = null;
    if (columnFormatSpec != null) {
      if (columnFormatSpec.getMultiValueHandling() != null) {
        mvh = columnFormatSpec.getMultiValueHandling();
      }
      if (columnFormatSpec.getIndexType() != null) {
        bitmap = columnFormatSpec.getIndexType().hasBitmapIndex();
      }
      maxStringLength = columnFormatSpec.getMaxStringLength();
    }
    return new StringDimensionHandler(getName(), mvh, bitmap, false, maxStringLength);
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
    if (!super.equals(o)) {
      return false;
    }
    StringDimensionSchema that = (StringDimensionSchema) o;
    return Objects.equals(columnFormatSpec, that.columnFormatSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), columnFormatSpec);
  }
}
