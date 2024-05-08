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

/**
 * Fault thrown when the user tries to insert strings with \u0000 (NULL) byte, since that is unsupported by row-based
 * frames
 */
@JsonTypeName(InvalidNullByteFault.CODE)
public class InvalidNullByteFault extends BaseMSQFault
{
  static final String CODE = "InvalidNullByte";

  private final String source;
  private final Integer rowNumber;
  private final String column;
  private final String value;
  private final Integer position;

  /**
   * All the parameters to the constructor can be null, in case we are unable to extract them from the site
   * where the error was generated
   * @param source      source where \0000 containing string was found
   * @param rowNumber   rowNumber where the \0000 containing string was found (1-indexed)
   * @param column      column name where the \0000 containing string was found
   * @param value       value of the \0000 containing string
   * @param position    position (1-indexed) of \0000 in the string. This is added in case the test viewer skips or
   *                    doesn't render \0000 correctly
   */
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
        "Invalid null byte at source[%s], rowNumber[%d], column[%s], value[%s], position[%d]. "
        + "Consider sanitizing the input string column using REPLACE(\"%s\", U&'\\0000', '') AS %s",
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
