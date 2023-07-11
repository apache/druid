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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

import java.util.Objects;

@JsonTypeName(InsertCannotOrderByDescendingFault.CODE)
public class InsertCannotOrderByDescendingFault extends BaseMSQFault
{
  static final String CODE = "InsertCannotOrderByDescending";

  private final String columnName;

  @JsonCreator
  public InsertCannotOrderByDescendingFault(
      @JsonProperty("columnName") final String columnName
  )
  {
    super(CODE, "Cannot ingest column [%s] in descending order", columnName);
    this.columnName = Preconditions.checkNotNull(columnName, "columnName");
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
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
    InsertCannotOrderByDescendingFault that = (InsertCannotOrderByDescendingFault) o;
    return Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), columnName);
  }
}
