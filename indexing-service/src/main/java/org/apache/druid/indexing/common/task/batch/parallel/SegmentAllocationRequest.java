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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Segment allocation request used in supervisor-task-based segment allocation protocol.
 *
 * @see SinglePhaseParallelIndexTaskRunner#allocateNewSegment(String, DateTime, String, String)
 */
public class SegmentAllocationRequest
{
  private final DateTime timestamp;
  private final String sequenceName;
  @Nullable
  private final String prevSegmentId;

  @JsonCreator
  public SegmentAllocationRequest(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("prevSegmentId") @Nullable String prevSegmentId
  )
  {
    this.timestamp = timestamp;
    this.sequenceName = sequenceName;
    this.prevSegmentId = prevSegmentId;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  @Nullable
  public String getPrevSegmentId()
  {
    return prevSegmentId;
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
    SegmentAllocationRequest request = (SegmentAllocationRequest) o;
    return Objects.equals(timestamp, request.timestamp) && Objects.equals(
        sequenceName,
        request.sequenceName
    ) && Objects.equals(prevSegmentId, request.prevSegmentId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamp, sequenceName, prevSegmentId);
  }
}
