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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.timeline.SegmentId;

import java.util.List;
import java.util.Map;

public class SinksSchema
{
  private final Map<Integer, ColumnInformation> columnMapping;

  private final Map<SegmentId, SinkSchemaChange> sinksSchemaChangeMap;

  public SinksSchema(
      Map<Integer, ColumnInformation> columnMapping,
      Map<SegmentId, SinkSchemaChange> sinksSchemaChangeMap
  )
  {
    this.columnMapping = columnMapping;
    this.sinksSchemaChangeMap = sinksSchemaChangeMap;
  }

  public Map<Integer, ColumnInformation> getColumnMapping()
  {
    return columnMapping;
  }

  public Map<SegmentId, SinkSchemaChange> getSinksSchemaChangeMap()
  {
    return sinksSchemaChangeMap;
  }

  public static class SinkSchemaChange
  {
    boolean delta;
    List<Integer> newColumns;
    List<Integer> updatedColumns;
    Integer numRows;

    public SinkSchemaChange(
        List<Integer> newColumns,
        List<Integer> updatedColumns,
        Integer numRows,
        boolean delta
    )
    {
      this.newColumns = newColumns;
      this.updatedColumns = updatedColumns;
      this.numRows = numRows;
      this.delta = delta;
    }

    public boolean isDelta()
    {
      return delta;
    }

    public List<Integer> getNewColumns()
    {
      return newColumns;
    }

    public List<Integer> getUpdatedColumns()
    {
      return updatedColumns;
    }

    public Integer getNumRows()
    {
      return numRows;
    }
  }

  public static class ColumnInformation
  {
    String columnName;

    ColumnType columnType;

    public ColumnInformation(String columnName, ColumnType columnType)
    {
      this.columnName = columnName;
      this.columnType = columnType;
    }

    public String getColumnName()
    {
      return columnName;
    }

    public ColumnType getColumnType()
    {
      return columnType;
    }
  }
}
