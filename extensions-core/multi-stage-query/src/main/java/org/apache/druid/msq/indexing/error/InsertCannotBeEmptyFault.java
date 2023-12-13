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

@JsonTypeName(InsertCannotBeEmptyFault.CODE)
public class InsertCannotBeEmptyFault extends BaseMSQFault
{
  public static final String CODE = "InsertCannotBeEmpty";

  private final String dataSource;

  @JsonCreator
  public InsertCannotBeEmptyFault(
      @JsonProperty("dataSource") final String dataSource
  )
  {
    super(CODE, "No rows to insert for dataSource[%s]. Set failOnEmptyInsert : false"
                + " in the query context to allow empty inserts.", dataSource);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
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
    InsertCannotBeEmptyFault that = (InsertCannotBeEmptyFault) o;
    return Objects.equals(dataSource, that.dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), dataSource);
  }
}
