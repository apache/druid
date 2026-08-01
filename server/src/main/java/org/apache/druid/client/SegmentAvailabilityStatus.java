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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * The Coordinator's answer to "should this segment be queryable right now?", for a segment a Broker has found itself
 * with no server for.
 * <p>
 * A Broker cannot tell on its own whether a segment that vanished from its timeline was legitimately removed or has
 * gone missing, so it asks the Coordinator, which owns both the used-segment set and the load rules. See
 * apache/druid#18716.
 */
public class SegmentAvailabilityStatus
{
  /**
   * Answer for a segment the Coordinator has no record of at all, which is what a Broker sees for a segment that was
   * legitimately killed or marked unused.
   */
  public static final SegmentAvailabilityStatus UNUSED = new SegmentAvailabilityStatus(false, null);

  private final boolean used;
  @Nullable
  private final Integer replicationFactor;

  @JsonCreator
  public SegmentAvailabilityStatus(
      @JsonProperty("used") boolean used,
      @JsonProperty("replicationFactor") @Nullable Integer replicationFactor
  )
  {
    this.used = used;
    this.replicationFactor = replicationFactor;
  }

  @JsonProperty
  public boolean isUsed()
  {
    return used;
  }

  /**
   * Number of replicas the load rules require, or null if the Coordinator has not evaluated rules for this segment
   * yet (for instance because it has only just become the leader).
   */
  @JsonProperty
  @Nullable
  public Integer getReplicationFactor()
  {
    return replicationFactor;
  }

  /**
   * Whether some server ought to be serving this segment.
   * <p>
   * A used segment whose rules ask for zero replicas is deliberately not loaded anywhere -- it is queryable only from
   * deep storage -- so its absence from the timeline is expected and must not be reported as unavailable.
   */
  public boolean isExpectedToBeAvailable()
  {
    return used && replicationFactor != null && replicationFactor > 0;
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
    SegmentAvailabilityStatus that = (SegmentAvailabilityStatus) o;
    return used == that.used && Objects.equals(replicationFactor, that.replicationFactor);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(used, replicationFactor);
  }

  @Override
  public String toString()
  {
    return "SegmentAvailabilityStatus{used=" + used + ", replicationFactor=" + replicationFactor + '}';
  }
}
