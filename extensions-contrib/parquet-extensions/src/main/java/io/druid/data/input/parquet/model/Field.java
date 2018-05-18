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
package io.druid.data.input.parquet.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.builder.ToStringBuilder;

public final class Field
{
  private static final int DEFAULT_INDEX = -1;

  private final String rootFieldName;
  private final Integer index;
  private final Utf8 key;
  private final FieldType fieldType;
  private final Field field;
  private final String dimensionName;

  @JsonCreator
  Field(
      @JsonProperty("rootFieldName") String rootFieldName,
      @JsonProperty("index") Integer index,
      @JsonProperty(value = "key") Utf8 key,
      @JsonProperty(value = "fieldType") FieldType fieldType,
      @JsonProperty("field") Field field,
      @JsonProperty("dimensionName") String dimensionName
  )
  {
    this.rootFieldName = rootFieldName;
    this.index = index == null ? DEFAULT_INDEX : index;
    this.key = key;
    this.fieldType = fieldType;
    this.field = field;
    this.dimensionName = dimensionName;
    //Having the null check at last to give more clarity on what field the error was!
    Preconditions.checkNotNull(this.key, String.format("ParquetParser field [%s] cannot have null field key ", this));
    Preconditions.checkNotNull(
        this.fieldType,
        String.format("ParquetParser field [%s] cannot have null FieldType ", this)
    );
  }

  public String getRootFieldName()
  {
    return rootFieldName;
  }

  public int getIndex()
  {
    return index;
  }

  public Utf8 getKey()
  {
    return key;
  }

  public FieldType getFieldType()
  {
    return fieldType;
  }

  public Field getField()
  {
    return field;
  }

  public String getDimensionName()
  {
    return dimensionName;
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

    Field field = (Field) o;

    if (!index.equals(field.index)) {
      return false;
    }
    if (rootFieldName != null ? !rootFieldName.equals(field.rootFieldName) : field.rootFieldName != null) {
      return false;
    }
    if (key != null ? !key.equals(field.key) : field.key != null) {
      return false;
    }
    return fieldType == field.fieldType;

  }

  @Override
  public int hashCode()
  {
    int result = rootFieldName != null ? rootFieldName.hashCode() : 0;
    result = 31 * result + index;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (fieldType != null ? fieldType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this)
        .append("rootFieldName", rootFieldName)
        .append("index", index)
        .append("key", key)
        .append("fieldType", fieldType)
        .append("field", field)
        .append("dimensionName", dimensionName)
        .toString();
  }
}
