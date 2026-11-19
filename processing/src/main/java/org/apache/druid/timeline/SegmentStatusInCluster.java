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
 * This class represents the current state of a segment in the cluster and encapsulates the following:
 * <ul>
 *   <li>the {@code DataSegment} object</li>
 *   <li>overshadowed status of the segment</li>
 *   <li>replication factor of the segment</li>
 *   <li>number of rows in the segment</li>
 *   <li>if the segment is realtime</li>
 *   <li>if the segment is published</li>
 * </ul>
 * <br>
 * Objects of this class are used to sync the state of segments from the Coordinator to different services, typically the Broker.
 * The {@link #compareTo} method considers only the {@link SegmentId}.
 */
public class SegmentStatusInCluster implements Comparable<SegmentStatusInCluster>
{
  private final boolean overshadowed;
  /**
   * The replication factor for the segment added across all tiers. This value is null if the load rules for
   * the segment have not been evaluated yet.
   */
  private final Integer replicationFactor;
  /**
   * dataSegment is serialized "unwrapped", i.e. it's properties are included as properties of
   * enclosing class. If in the future, if {@code SegmentStatusInCluster} were to extend {@link DataSegment},
   * there will be no change in the serialized format.
   */
  @JsonUnwrapped
  private final DataSegment dataSegment;
  /**
   * Number of rows in the segment.
   */
  private final Long numRows;
  /**
   * If the segment is realtime.
   */
  private final boolean realtime;
  /**
   * If the segment is published, that is, present in the metadata store and marked as used.
   */
  private final boolean published;

  @JsonCreator
  public SegmentStatusInCluster(
      @JsonProperty("overshadowed") boolean overshadowed,
      @JsonProperty("replicationFactor") @Nullable Integer replicationFactor,
      @JsonProperty("numRows") @Nullable Long numRows,
      @JsonProperty("realtime") boolean realtime,
      @JsonProperty("published") @Nullable Boolean published
  )
  {
    // Jackson will overwrite dataSegment if needed (even though the field is final)
    this(null, overshadowed, replicationFactor, numRows, realtime, published);
  }

  /**
   * Creates a status whose published flag is the opposite of {@code realtime}.
   */
  public SegmentStatusInCluster(
      DataSegment dataSegment,
      boolean overshadowed,
      Integer replicationFactor,
      Long numRows,
      boolean realtime
  )
  {
    this(dataSegment, overshadowed, replicationFactor, numRows, realtime, null);
  }

  /**
   * @param published whether the segment is published. Null if the sender does not say, as with a Coordinator on
   *                  an older version. The segment is then taken to be published if it is not realtime.
   */
  public SegmentStatusInCluster(
      DataSegment dataSegment,
      boolean overshadowed,
      Integer replicationFactor,
      Long numRows,
      boolean realtime,
      @Nullable Boolean published
  )
  {
    this.dataSegment = dataSegment;
    this.overshadowed = overshadowed;
    this.replicationFactor = replicationFactor;
    this.numRows = numRows;
    this.realtime = realtime;
    this.published = published == null ? !realtime : published;
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
  public Integer getReplicationFactor()
  {
    return replicationFactor;
  }

  @Nullable
  @JsonProperty
  public Long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public boolean isRealtime()
  {
    return realtime;
  }

  @JsonProperty
  public boolean isPublished()
  {
    return published;
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
    SegmentStatusInCluster that = (SegmentStatusInCluster) o;
    return overshadowed == that.overshadowed
           && realtime == that.realtime
           && published == that.published
           && Objects.equals(replicationFactor, that.replicationFactor)
           && Objects.equals(dataSegment, that.dataSegment)
           && Objects.equals(numRows, that.numRows);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(overshadowed, replicationFactor, dataSegment, numRows, realtime, published);
  }

  @Override
  public int compareTo(SegmentStatusInCluster o)
  {
    return dataSegment.getId().compareTo(o.dataSegment.getId());
  }

  @Override
  public String toString()
  {
    return "SegmentStatusInCluster{" +
           "overshadowed=" + overshadowed +
           ", replicationFactor=" + replicationFactor +
           ", dataSegment=" + dataSegment +
           ", numRows=" + numRows +
           ", realtime=" + realtime +
           ", published=" + published +
           '}';
  }
}
