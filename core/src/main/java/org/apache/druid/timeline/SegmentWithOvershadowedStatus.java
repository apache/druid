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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DataSegment object plus the overshadowed status for the segment. An immutable object.
 *
 * SegmentWithOvershadowedStatus's {@link #compareTo} method considers only the {@link SegmentId}
 * of the DataSegment object.
 */
public class SegmentWithOvershadowedStatus implements Comparable<SegmentWithOvershadowedStatus>
{
  private final boolean overshadowed;
  private final DataSegment dataSegment;

  @JsonCreator
  public SegmentWithOvershadowedStatus(
      @JsonProperty("dataSegment") DataSegment dataSegment,
      @JsonProperty("overshadowed") boolean overshadowed
  )
  {
    this.dataSegment = dataSegment;
    this.overshadowed = overshadowed;
  }

  @JsonProperty
  public boolean isOvershadowed()
  {
    return overshadowed;
  }

  @JsonProperty
  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentWithOvershadowedStatus)) {
      return false;
    }
    final SegmentWithOvershadowedStatus that = (SegmentWithOvershadowedStatus) o;
    if (!dataSegment.equals(that.dataSegment)) {
      return false;
    }
    if (overshadowed != (that.overshadowed)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dataSegment.hashCode();
    result = 31 * result + Boolean.hashCode(overshadowed);
    return result;
  }

  @Override
  public int compareTo(SegmentWithOvershadowedStatus o)
  {
    return dataSegment.getId().compareTo(o.dataSegment.getId());
  }
}
