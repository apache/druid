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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.ColumnType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates schema information for multiple segments.
 * <p>
 * Primarily used to announce schema changes for all {@link org.apache.druid.segment.realtime.plumber.Sink}
 * created by a task in {@link StreamAppenderator}.
 */
public class SegmentSchemas
{
  private final List<SegmentSchema> segmentSchemaList;

  @JsonCreator
  public SegmentSchemas(
      @JsonProperty("segmentSchemaList") List<SegmentSchema> segmentSchemaList
  )
  {
    this.segmentSchemaList = segmentSchemaList;
  }

  @JsonProperty
  public List<SegmentSchema> getSegmentSchemaList()
  {
    return segmentSchemaList;
  }

  /**
   * Encapsulates either the absolute schema or schema change for a segment.
   */
  public static class SegmentSchema
  {
    private final String dataSource;
    private final String segmentId;

    // represents whether it is a schema change or absolute schema
    private final boolean delta;

    // absolute number of rows in the segment
    private final Integer numRows;

    // new columns in the segment
    private final List<String> newColumns;

    // updated column types, empty for absolute segment schema
    private final List<String> updatedColumns;

    // all column should have non-null types
    private final Map<String, ColumnType> columnTypeMap;

    @JsonCreator
    public SegmentSchema(
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("segmentId") String segmentId,
        @JsonProperty("delta") boolean delta,
        @JsonProperty("numRows") Integer numRows,
        @JsonProperty("newColumns") List<String> newColumns,
        @JsonProperty("updatedColumns") List<String> updatedColumns,
        @JsonProperty("columnTypeMap") Map<String, ColumnType> columnTypeMap
    )
    {
      this.dataSource = dataSource;
      this.segmentId = segmentId;
      this.delta = delta;
      this.numRows = numRows;
      this.newColumns = newColumns;
      this.updatedColumns = updatedColumns;
      this.columnTypeMap = columnTypeMap;
    }

    @JsonProperty
    public String getDataSource()
    {
      return dataSource;
    }

    @JsonProperty
    public String getSegmentId()
    {
      return segmentId;
    }

    @JsonProperty
    public boolean isDelta()
    {
      return delta;
    }

    @JsonProperty
    public Integer getNumRows()
    {
      return numRows;
    }

    @JsonProperty
    public List<String> getNewColumns()
    {
      return newColumns;
    }

    @JsonProperty
    public List<String> getUpdatedColumns()
    {
      return updatedColumns;
    }

    @JsonProperty
    public Map<String, ColumnType> getColumnTypeMap()
    {
      return columnTypeMap;
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
      SegmentSchema that = (SegmentSchema) o;
      return delta == that.delta
             && Objects.equals(dataSource, that.dataSource)
             && Objects.equals(segmentId, that.segmentId)
             && Objects.equals(numRows, that.numRows)
             && Objects.equals(newColumns, that.newColumns)
             && Objects.equals(updatedColumns, that.updatedColumns)
             && Objects.equals(columnTypeMap, that.columnTypeMap);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(dataSource, segmentId, delta, numRows, newColumns, updatedColumns, columnTypeMap);
    }

    @Override
    public String toString()
    {
      return "SegmentSchema{" +
             "dataSource='" + dataSource + '\'' +
             ", segmentId='" + segmentId + '\'' +
             ", delta=" + delta +
             ", numRows=" + numRows +
             ", newColumns=" + newColumns +
             ", updatedColumns=" + updatedColumns +
             ", columnTypeMap=" + columnTypeMap +
             '}';
    }
  }

  @Override
  public String toString()
  {
    return "SegmentSchemas{" +
           "segmentSchemaList=" + segmentSchemaList +
           '}';
  }
}
