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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import org.apache.druid.segment.Metadata;

import java.util.List;
import java.util.Objects;

/**
 * V10 Segment format projection metadata, containing projection row counts and schema information
 */
public class ProjectionMetadata
{
  public static final Interner<ProjectionSchema> SCHEMA_INTERNER = Interners.newWeakInterner();

  public static ProjectionMetadata forBaseTable(int numRows, List<String> dims, Metadata metadata)
  {
    final ProjectionSchema schema;
    List<String> dimsWithTime = Lists.newArrayListWithCapacity(dims.size() + 1);
    for (int i = 0; i < dims.size() + 1; i++) {
      dimsWithTime.add(metadata.getOrdering().get(i).getColumnName());
    }
    if (Boolean.TRUE.equals(metadata.isRollup())) {
      schema = RollupTableProjectionSchema.fromMetadata(dimsWithTime, metadata);
    } else {
      schema = TableProjectionSchema.fromMetadata(dimsWithTime, metadata);
    }
    return new ProjectionMetadata(
        numRows,
        schema
    );
  }

  private final int numRows;
  private final ProjectionSchema schema;

  @JsonCreator
  public ProjectionMetadata(
      @JsonProperty("numRows") int numRows,
      @JsonProperty("schema") ProjectionSchema schema
  )
  {
    this.numRows = numRows;
    this.schema = SCHEMA_INTERNER.intern(schema);
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public ProjectionSchema getSchema()
  {
    return schema;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectionMetadata that = (ProjectionMetadata) o;
    return numRows == that.numRows && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numRows, schema);
  }

  @Override
  public String toString()
  {
    return "ProjectionMetadata{" +
           "numRows=" + numRows +
           ", schema=" + schema +
           '}';
  }
}
