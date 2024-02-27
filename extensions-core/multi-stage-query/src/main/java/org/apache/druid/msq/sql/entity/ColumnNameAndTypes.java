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

package org.apache.druid.msq.sql.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * The column name and its sql {@link org.apache.calcite.sql.type.SqlTypeName} and its native {@link org.apache.druid.segment.column.ColumnType}
 */

public class ColumnNameAndTypes
{

  private final String colName;
  private final String sqlTypeName;

  private final String nativeTypeName;

  @JsonCreator
  public ColumnNameAndTypes(
      @JsonProperty("name") String colName,
      @JsonProperty("type") String sqlTypeName,
      @JsonProperty("nativeType") String nativeTypeName
  )
  {

    this.colName = colName;
    this.sqlTypeName = sqlTypeName;
    this.nativeTypeName = nativeTypeName;
  }

  @JsonProperty("name")
  public String getColName()
  {
    return colName;
  }

  @JsonProperty("type")
  public String getSqlTypeName()
  {
    return sqlTypeName;
  }

  @JsonProperty("nativeType")
  public String getNativeTypeName()
  {
    return nativeTypeName;
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
    ColumnNameAndTypes that = (ColumnNameAndTypes) o;
    return Objects.equals(colName, that.colName)
           && Objects.equals(sqlTypeName, that.sqlTypeName)
           && Objects.equals(nativeTypeName, that.nativeTypeName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(colName, sqlTypeName, nativeTypeName);
  }

  @Override
  public String toString()
  {
    return "ColumnNameAndTypes{" +
           "colName='" + colName + '\'' +
           ", sqlTypeName='" + sqlTypeName + '\'' +
           ", nativeTypeName='" + nativeTypeName + '\'' +
           '}';
  }
}


