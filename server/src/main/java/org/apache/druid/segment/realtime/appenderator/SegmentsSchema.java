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
import org.apache.druid.timeline.SegmentId;

import java.util.List;
import java.util.Map;

/**
 * Encapsulates schema information for multiple segments.
 * Optimise for storage by mapping column information to integer value.
 * <p>
 * Primarily used to announce schema changes for all {@link org.apache.druid.segment.realtime.plumber.Sink}
 * created by a task in {@link StreamAppenderator}.
 */
public class SegmentsSchema
{
  // integer value to column information mapping
  private final Map<Integer, ColumnInformation> columnMapping;

  // segmentId to schema mapping
  private final Map<SegmentId, SegmentSchema> segmentSchemaMap;

  @JsonCreator
  public SegmentsSchema(
      @JsonProperty("columnMapping") Map<Integer, ColumnInformation> columnMapping,
      @JsonProperty("segmentSchemaChangeMap") Map<SegmentId, SegmentSchema> segmentSchemaMap
  )
  {
    this.columnMapping = columnMapping;
    this.segmentSchemaMap = segmentSchemaMap;
  }

  @JsonProperty
  public Map<Integer, ColumnInformation> getColumnMapping()
  {
    return columnMapping;
  }

  @JsonProperty
  public Map<SegmentId, SegmentSchema> getSegmentSchemaMap()
  {
    return segmentSchemaMap;
  }

  /**
   * Encapsulates either the absolute schema or schema change for a segment.
   */
  public static class SegmentSchema
  {
    // represents whether it is a schema change or absolute schema
    boolean delta;
    // absolute number of rows in the segment
    Integer numRows;
    // new columns in the segment
    List<Integer> newColumns;
    // updated column types, empty for absolute segment schema
    List<Integer> updatedColumns;

    @JsonCreator
    public SegmentSchema(
        @JsonProperty("delta") boolean delta,
        @JsonProperty("numRows") Integer numRows,
        @JsonProperty("newColumns") List<Integer> newColumns,
        @JsonProperty("updatedColumns") List<Integer> updatedColumns
    )
    {
      this.newColumns = newColumns;
      this.updatedColumns = updatedColumns;
      this.numRows = numRows;
      this.delta = delta;
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
    public List<Integer> getNewColumns()
    {
      return newColumns;
    }

    @JsonProperty
    public List<Integer> getUpdatedColumns()
    {
      return updatedColumns;
    }

    @Override
    public String toString()
    {
      return "SegmentSchema{" +
             "delta=" + delta +
             ", numRows=" + numRows +
             ", newColumns=" + newColumns +
             ", updatedColumns=" + updatedColumns +
             '}';
    }
  }

  /**
   * Encapsulates column name and type.
   */
  public static class ColumnInformation
  {
    String columnName;
    ColumnType columnType;

    @JsonCreator
    public ColumnInformation(
        @JsonProperty("columnName") String columnName,
        @JsonProperty("columnName") ColumnType columnType)
    {
      this.columnName = columnName;
      this.columnType = columnType;
    }

    @JsonProperty
    public String getColumnName()
    {
      return columnName;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
      return columnType;
    }

    @Override
    public String toString()
    {
      return "ColumnInformation{" +
             "columnName='" + columnName + '\'' +
             ", columnType=" + columnType +
             '}';
    }
  }

  @Override
  public String toString()
  {
    return "SegmentsSchema{" +
           "columnMapping=" + columnMapping +
           ", segmentSchemaChangeMap=" + segmentSchemaMap +
           '}';
  }
}
