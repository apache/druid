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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.msq.input.InputSlice;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Input slice representing a set of segments to read.
 * <br>
 * Sliced from {@link TableInputSpec} by {@link TableInputSpecSlicer}.
 * <br>
 * Similar to {@link org.apache.druid.query.spec.MultipleSpecificSegmentSpec} from native queries.
 * <br>
 * These use {@link RichSegmentDescriptor}, not {@link org.apache.druid.timeline.DataSegment}, to minimize overhead
 * in scenarios where the target server already has the segment cached. If the segment isn't cached, the target
 * server does need to fetch the full {@link org.apache.druid.timeline.DataSegment} object, so it can get the
 * {@link org.apache.druid.segment.loading.LoadSpec} and fetch the segment from deep storage.
 */
@JsonTypeName("segments")
public class SegmentsInputSlice implements InputSlice
{
  private final String dataSource;
  private final List<RichSegmentDescriptor> descriptors;
  private final List<DataServerRequestDescriptor> servedSegments;

  @JsonCreator
  public SegmentsInputSlice(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<RichSegmentDescriptor> descriptors,
      @JsonProperty("servedSegments") @Nullable List<DataServerRequestDescriptor> servedSegments
  )
  {
    this.dataSource = dataSource;
    this.descriptors = descriptors;
    this.servedSegments = servedSegments;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("segments")
  public List<RichSegmentDescriptor> getDescriptors()
  {
    return descriptors;
  }

  @Nullable
  @JsonProperty("servedSegments")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<DataServerRequestDescriptor> getServedSegments()
  {
    return servedSegments;
  }

  @Override
  public int fileCount()
  {
    return descriptors.size();
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
    SegmentsInputSlice that = (SegmentsInputSlice) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(
        descriptors,
        that.descriptors
    ) && Objects.equals(servedSegments, that.servedSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, descriptors, servedSegments);
  }

  @Override
  public String toString()
  {
    return "SegmentsInputSlice{" +
           "dataSource='" + dataSource + '\'' +
           ", descriptors=" + descriptors +
           ", servedSegments=" + servedSegments +
           '}';
  }
}
