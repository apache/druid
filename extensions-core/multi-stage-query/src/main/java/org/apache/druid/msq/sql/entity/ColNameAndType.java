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


public class ColNameAndType
{

  private final String colName;
  private final String sqlTypeName;

  @JsonCreator
  public ColNameAndType(@JsonProperty("name") String colName, @JsonProperty("type") String sqlTypeName)
  {

    this.colName = colName;
    this.sqlTypeName = sqlTypeName;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColNameAndType that = (ColNameAndType) o;
    return Objects.equals(colName, that.colName) && Objects.equals(sqlTypeName, that.sqlTypeName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(colName, sqlTypeName);
  }

  @Override
  public String toString()
  {
    return "ColNameAndType{" +
           "colName='" + colName + '\'' +
           ", sqlTypeName='" + sqlTypeName + '\'' +
           '}';
  }
}


