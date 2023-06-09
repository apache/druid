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
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * DataSegment object plus the overshadowed and target number of replicants for the segment. An immutable object.
 * <br></br>
 * SegmentPlus's {@link #compareTo} method considers only the {@link SegmentId}
 * of the DataSegment object.
 */
public class SegmentPlus implements Comparable<SegmentPlus>
{
  private final boolean overshadowed;
  /**
   * The target number of replicants for the segment added across all tiers. This value is null if the load rules for
   * the segment have not been evaluated yet.
   */
  private final Integer totalTargetReplicants;
  /**
   * dataSegment is serialized "unwrapped", i.e. it's properties are included as properties of
   * enclosing class. If in the future, if {@code SegmentPlus} were to extend {@link DataSegment},
   * there will be no change in the serialized format.
   */
  @JsonUnwrapped
  private final DataSegment dataSegment;

  @JsonCreator
  public SegmentPlus(
      @JsonProperty("overshadowed") boolean overshadowed,
      @JsonProperty("totalTargetReplicants") Integer targetReplicants
  )
  {
    // Jackson will overwrite dataSegment if needed (even though the field is final)
    this(null, overshadowed, targetReplicants);
  }

  public SegmentPlus(
      DataSegment dataSegment,
      boolean overshadowed,
      Integer totalTargetReplicants
  )
  {
    this.dataSegment = dataSegment;
    this.overshadowed = overshadowed;
    this.totalTargetReplicants = totalTargetReplicants;
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

  @Nullable
  @JsonProperty
  public Integer getTotalTargetReplicants()
  {
    return totalTargetReplicants;
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
    SegmentPlus that = (SegmentPlus) o;
    return overshadowed == that.overshadowed
           && Objects.equals(totalTargetReplicants, that.totalTargetReplicants)
           && Objects.equals(dataSegment, that.dataSegment);
  }

  @Override
  public int hashCode()
  {
    int result = dataSegment.hashCode();
    result = 31 * result + Objects.hash(overshadowed, totalTargetReplicants);
    return result;
  }

  @Override
  public int compareTo(SegmentPlus o)
  {
    return dataSegment.getId().compareTo(o.dataSegment.getId());
  }

  @Override
  public String toString()
  {
    return "SegmentPlus{" +
           "overshadowed=" + overshadowed +
           ", totalTargetReplicants=" + totalTargetReplicants +
           ", dataSegment=" + dataSegment +
           '}';
  }
}
