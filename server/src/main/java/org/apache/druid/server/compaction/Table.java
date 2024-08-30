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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A simple table POJO with any number of rows and specified column names.
 * Used in {@link CompactionSimulateResult}.
 */
public class Table
{
  private final List<String> columnNames;
  private final List<List<Object>> rows = new ArrayList<>();

  public static Table withColumnNames(String... columnNames)
  {
    return new Table(Arrays.asList(columnNames), null);
  }

  @JsonCreator
  public Table(
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("rows") List<List<Object>> rows
  )
  {
    this.columnNames = columnNames;
    if (rows != null) {
      this.rows.addAll(rows);
    }
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @JsonProperty
  public List<List<Object>> getRows()
  {
    return rows;
  }

  public void addRow(Object... values)
  {
    rows.add(Arrays.asList(values));
  }

  public boolean isEmpty()
  {
    return rows.isEmpty();
  }

  @Override
  public String toString()
  {
    return "Table{" +
           "columnNames=" + columnNames +
           ", rows=" + rows +
           '}';
  }
}
