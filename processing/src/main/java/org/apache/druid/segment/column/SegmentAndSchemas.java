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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulates segment metadata and corresponding schema.
 */
public class SegmentAndSchemas
{
  private final Set<DataSegment> segments;
  private final MinimalSegmentSchemas minimalSegmentSchemas;

  public SegmentAndSchemas()
  {
    this.segments = new HashSet<>();
    this.minimalSegmentSchemas = new MinimalSegmentSchemas();
  }

  @JsonCreator
  public SegmentAndSchemas(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("minimalSegmentSchemas") MinimalSegmentSchemas minimalSegmentSchemas
  )
  {
    this.segments = segments;
    this.minimalSegmentSchemas = minimalSegmentSchemas;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public MinimalSegmentSchemas getMinimalSegmentSchemas()
  {
    return minimalSegmentSchemas;
  }

  public SegmentAndSchemas merge(SegmentAndSchemas other)
  {
    segments.addAll(other.getSegments());
    minimalSegmentSchemas.merge(other.getMinimalSegmentSchemas());
    return this;
  }
}
