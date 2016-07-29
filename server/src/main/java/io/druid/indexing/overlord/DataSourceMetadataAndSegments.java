/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.druid.timeline.DataSegment;

import java.util.Set;

/**
 */
public class DataSourceMetadataAndSegments
{
  private final Set<DataSegment> segments;
  private final DataSourceMetadata startMetadata;
  private final DataSourceMetadata endMetadata;

  @JsonCreator
  public DataSourceMetadataAndSegments(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("startMetadata") DataSourceMetadata startMetadata,
      @JsonProperty("endMetadata") DataSourceMetadata endMetadata
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
    this.startMetadata = startMetadata;
    this.endMetadata = endMetadata;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public DataSourceMetadata getStartMetadata()
  {
    return startMetadata;
  }

  @JsonProperty
  public DataSourceMetadata getEndMetadata()
  {
    return endMetadata;
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

    DataSourceMetadataAndSegments that = (DataSourceMetadataAndSegments) o;

    if (!segments.equals(that.segments)) {
      return false;
    }
    if (startMetadata != null ? !startMetadata.equals(that.startMetadata) : that.startMetadata != null) {
      return false;
    }
    return !(endMetadata != null ? !endMetadata.equals(that.endMetadata) : that.endMetadata != null);

  }

  @Override
  public int hashCode()
  {
    int result = segments.hashCode();
    result = 31 * result + (startMetadata != null ? startMetadata.hashCode() : 0);
    result = 31 * result + (endMetadata != null ? endMetadata.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DataSourceMetadataAndSegments{" +
           "segments=" + segments +
           ", startMetadata=" + startMetadata +
           ", endMetadata=" + endMetadata +
           '}';
  }
}
