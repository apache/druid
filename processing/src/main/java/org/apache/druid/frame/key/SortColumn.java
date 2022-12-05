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

package org.apache.druid.frame.key;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

/**
 * Represents a component of an order-by key.
 */
public class SortColumn
{
  private final String columnName;
  private final boolean descending;

  @JsonCreator
  public SortColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("descending") boolean descending
  )
  {
    if (columnName == null || columnName.isEmpty()) {
      throw new IAE("Cannot have null or empty column name");
    }

    this.columnName = columnName;
    this.descending = descending;
  }

  @JsonProperty
  public String columnName()
  {
    return columnName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean descending()
  {
    return descending;
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
    SortColumn that = (SortColumn) o;
    return descending == that.descending && Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, descending);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s%s", columnName, descending ? " DESC" : "");
  }
}
