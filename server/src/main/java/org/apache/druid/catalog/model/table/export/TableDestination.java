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

package org.apache.druid.catalog.model.table.export;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.model.table.IngestDestination;

import java.util.Objects;

public class TableDestination implements IngestDestination
{
  public static final String TYPE_KEY = "table";
  String tableName;

  @JsonCreator
  public TableDestination(@JsonProperty("tableName") String tableName)
  {
    this.tableName = tableName;
  }

  @Override
  @JsonProperty("tableName")
  public String getDestinationName()
  {
    return tableName;
  }

  @Override
  public String toString()
  {
    return tableName;
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
    TableDestination that = (TableDestination) o;
    return Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tableName);
  }
}
