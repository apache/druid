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
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * This immutable class encapsulates segments metadata and corresponding schema.
 */
public class DataSegmentsWithSchemas
{
  private final Set<DataSegment> segments;

  @Nullable
  private final SegmentSchemaMapping segmentSchemaMapping;

  public DataSegmentsWithSchemas(int schemaVersion)
  {
    this.segments = new HashSet<>();
    this.segmentSchemaMapping = new SegmentSchemaMapping(schemaVersion);
  }

  @JsonCreator
  public DataSegmentsWithSchemas(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("segmentSchemaMapping") @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    this.segments = segments;
    this.segmentSchemaMapping = segmentSchemaMapping;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @Nullable
  @JsonProperty
  public SegmentSchemaMapping getSegmentSchemaMapping()
  {
    return segmentSchemaMapping;
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
    DataSegmentsWithSchemas that = (DataSegmentsWithSchemas) o;
    return Objects.equals(segments, that.segments) && Objects.equals(
        segmentSchemaMapping,
        that.segmentSchemaMapping
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments, segmentSchemaMapping);
  }
}
