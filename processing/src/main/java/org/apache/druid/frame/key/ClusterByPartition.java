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

package org.apache.druid.frame.key;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Boundaries of a partition marked by start and end keys. The keys are generally described by a
 * {@link ClusterBy} instance that is not referenced here. (It is generally provided contextually.)
 *
 * Often, this object is part of a full partition set represented by {@link ClusterByPartitions}.
 */
public class ClusterByPartition
{
  @Nullable
  private final RowKey start;
  @Nullable
  private final RowKey end;

  @JsonCreator
  public ClusterByPartition(
      @JsonProperty("start") @Nullable RowKey start,
      @JsonProperty("end") @Nullable RowKey end
  )
  {
    this.start = start;
    this.end = end;
  }

  /**
   * Get the starting key for this range. It is inclusive (the range *does* contain this key).
   *
   * Null means the range is unbounded at the start.
   */
  @JsonProperty
  @Nullable
  public RowKey getStart()
  {
    return start;
  }

  /**
   * Get the ending key for this range. It is exclusive (the range *does not* contain this key).
   *
   * Null means the range is unbounded at the end.
   */
  @JsonProperty
  @Nullable
  public RowKey getEnd()
  {
    return end;
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
    ClusterByPartition that = (ClusterByPartition) o;
    return Objects.equals(start, that.start) && Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(start, end);
  }

  @Override
  public String toString()
  {
    return "[" + start + ", " + end + ")";
  }
}
