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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(InvalidNullByteFault.CODE)
public class InvalidNullByteFault extends BaseMSQFault
{
  static final String CODE = "InvalidNullByte";

  private final String source;
  private final Integer rowNumber;
  private final String column;
  private final String value;
  private final Integer position;

  @JsonCreator
  public InvalidNullByteFault(
      @Nullable @JsonProperty("source") final String source,
      @Nullable @JsonProperty("rowNumber") final Integer rowNumber,
      @Nullable @JsonProperty("column") final String column,
      @Nullable @JsonProperty("value") final String value,
      @Nullable @JsonProperty("position") final Integer position
  )
  {
    super(
        CODE,
        "Invalid null byte at source [%s], rowNumber [%d], column[%s], value[%s], position[%d]. Consider sanitizing the string using REPLACE(\"%s\", U&'\\0000', '') AS %s",
        source,
        rowNumber,
        column,
        value,
        position,
        column,
        column
    );
    this.source = source;
    this.rowNumber = rowNumber;
    this.column = column;
    this.value = value;
    this.position = position;
  }


  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSource()
  {
    return source;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getRowNumber()
  {
    return rowNumber;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getValue()
  {
    return value;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getColumn()
  {
    return column;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getPosition()
  {
    return position;
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
    InvalidNullByteFault that = (InvalidNullByteFault) o;
    return Objects.equals(source, that.source)
           && Objects.equals(rowNumber, that.rowNumber)
           && Objects.equals(column, that.column)
           && Objects.equals(value, that.value)
           && Objects.equals(position, that.position);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), source, rowNumber, column, value, position);
  }
}
