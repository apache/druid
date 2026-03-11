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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.ProjectionMetadata;

import java.util.Comparator;
import java.util.Objects;

/**
 * V9 Segment format aggregate projection schema and row count information to store in {@link Metadata} which itself
 * is stored inside a segment, defining which projections exist for the segment.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonTypeName(AggregateProjectionSpec.TYPE_NAME)
public class AggregateProjectionMetadata
{
  public static final Comparator<AggregateProjectionMetadata> COMPARATOR = (o1, o2) -> {
    int rowCompare = Integer.compare(o1.numRows, o2.numRows);
    if (rowCompare != 0) {
      return rowCompare;
    }
    return AggregateProjectionSchema.COMPARATOR.compare(o1.getSchema(), o2.getSchema());
  };

  private final AggregateProjectionSchema schema;
  private final int numRows;

  @JsonCreator
  public AggregateProjectionMetadata(
      @JsonProperty("schema") AggregateProjectionSchema schema,
      @JsonProperty("numRows") int numRows
  )
  {
    this.schema = (AggregateProjectionSchema) ProjectionMetadata.SCHEMA_INTERNER.intern(schema);
    this.numRows = numRows;
  }

  @JsonProperty
  public AggregateProjectionSchema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
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
    AggregateProjectionMetadata that = (AggregateProjectionMetadata) o;
    return numRows == that.numRows && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schema, numRows);
  }

  @Override
  public String toString()
  {
    return "AggregateProjectionMetadata{" +
           "schema=" + schema +
           ", numRows=" + numRows +
           '}';
  }
}
