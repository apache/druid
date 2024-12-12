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

package org.apache.druid.rpc.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class SegmentUpdateResponse
{
  private final int numChangedSegments;
  private final boolean segmentStateChanged;

  @JsonCreator
  public SegmentUpdateResponse(
      @JsonProperty("numChangedSegments") int numChangedSegments
  )
  {
    this.numChangedSegments = numChangedSegments;
    this.segmentStateChanged = numChangedSegments > 0;
  }

  @JsonProperty
  public int getNumChangedSegments()
  {
    return numChangedSegments;
  }

  /**
   * This field is required for backward compatibility of responses of
   * {@link org.apache.druid.server.http.DataSourcesResource#markSegmentAsUsed}
   * and {@link org.apache.druid.server.http.DataSourcesResource#markSegmentAsUnused}
   */
  @JsonProperty
  public boolean isSegmentStateChanged()
  {
    return segmentStateChanged;
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
    SegmentUpdateResponse response = (SegmentUpdateResponse) o;
    return numChangedSegments == response.numChangedSegments;
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(numChangedSegments);
  }

  @Override
  public String toString()
  {
    return "SegmentUpdateResponse{numChangedSegments=" + numChangedSegments + "}";
  }
}
