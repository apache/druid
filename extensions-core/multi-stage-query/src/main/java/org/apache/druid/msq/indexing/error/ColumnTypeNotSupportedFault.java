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
import com.google.common.base.Preconditions;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(ColumnTypeNotSupportedFault.CODE)
public class ColumnTypeNotSupportedFault extends BaseMSQFault
{
  static final String CODE = "ColumnTypeNotSupported";

  private final String columnName;

  @Nullable
  private final ColumnType columnType;

  @JsonCreator
  public ColumnTypeNotSupportedFault(
      @JsonProperty("columnName") final String columnName,
      @JsonProperty("columnType") @Nullable final ColumnType columnType
  )
  {
    super(CODE, UnsupportedColumnTypeException.message(columnName, columnType));
    this.columnName = Preconditions.checkNotNull(columnName, "columnName");
    this.columnType = columnType;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ColumnType getColumnType()
  {
    return columnType;
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
    ColumnTypeNotSupportedFault that = (ColumnTypeNotSupportedFault) o;
    return Objects.equals(columnName, that.columnName) && Objects.equals(columnType, that.columnType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), columnName, columnType);
  }
}
