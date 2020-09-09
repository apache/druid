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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Objects;

public class AutoCompactionSnapshot
{
  public enum AutoCompactionScheduleStatus
  {
    NOT_ENABLED,
    RUNNING
  }

  @JsonProperty
  private String dataSource;
  @JsonProperty
  private AutoCompactionScheduleStatus scheduleStatus;
  @JsonProperty
  private String latestScheduledTaskId;
  @JsonProperty
  private long byteAwaitingCompaction;
  @JsonProperty
  private long byteProcessed;
  @JsonProperty
  private long segmentCountAwaitingCompaction;
  @JsonProperty
  private long segmentCountProcessed;
  @JsonProperty
  private long intervalCountAwaitingCompaction;
  @JsonProperty
  private long intervalCountProcessed;

  @JsonCreator
  public AutoCompactionSnapshot(
      @JsonProperty @NotNull String dataSource,
      @JsonProperty @NotNull AutoCompactionScheduleStatus scheduleStatus
  )
  {
    this.dataSource = dataSource;
    this.scheduleStatus = scheduleStatus;
  }

  @NotNull
  public String getDataSource()
  {
    return dataSource;
  }

  @NotNull
  public AutoCompactionScheduleStatus getScheduleStatus()
  {
    return scheduleStatus;
  }

  @Nullable
  public String getLatestScheduledTaskId()
  {
    return latestScheduledTaskId;
  }

  public long getByteAwaitingCompaction()
  {
    return byteAwaitingCompaction;
  }

  public long getByteProcessed()
  {
    return byteProcessed;
  }

  public long getSegmentCountAwaitingCompaction()
  {
    return segmentCountAwaitingCompaction;
  }

  public long getSegmentCountProcessed()
  {
    return segmentCountProcessed;
  }

  public long getIntervalCountAwaitingCompaction()
  {
    return intervalCountAwaitingCompaction;
  }

  public long getIntervalCountProcessed()
  {
    return intervalCountProcessed;
  }

  public void setScheduleStatus(AutoCompactionScheduleStatus scheduleStatus)
  {
    this.scheduleStatus = scheduleStatus;
  }

  public void setLatestScheduledTaskId(String latestScheduledTaskId)
  {
    this.latestScheduledTaskId = latestScheduledTaskId;
  }

  public void setByteAwaitingCompaction(long byteAwaitingCompaction)
  {
    this.byteAwaitingCompaction = byteAwaitingCompaction;
  }

  public void setByteProcessed(long byteProcessed)
  {
    this.byteProcessed = byteProcessed;
  }

  public void setSegmentCountAwaitingCompaction(long segmentCountAwaitingCompaction)
  {
    this.segmentCountAwaitingCompaction = segmentCountAwaitingCompaction;
  }

  public void setSegmentCountProcessed(long segmentCountProcessed)
  {
    this.segmentCountProcessed = segmentCountProcessed;
  }

  public void setIntervalCountAwaitingCompaction(long intervalCountAwaitingCompaction)
  {
    this.intervalCountAwaitingCompaction = intervalCountAwaitingCompaction;
  }

  public void setIntervalCountProcessed(long intervalCountProcessed)
  {
    this.intervalCountProcessed = intervalCountProcessed;
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
    AutoCompactionSnapshot that = (AutoCompactionSnapshot) o;
    return byteAwaitingCompaction == that.byteAwaitingCompaction &&
           byteProcessed == that.byteProcessed &&
           segmentCountAwaitingCompaction == that.segmentCountAwaitingCompaction &&
           segmentCountProcessed == that.segmentCountProcessed &&
           intervalCountAwaitingCompaction == that.intervalCountAwaitingCompaction &&
           intervalCountProcessed == that.intervalCountProcessed &&
           dataSource.equals(that.dataSource) &&
           Objects.equals(scheduleStatus, that.scheduleStatus) &&
           Objects.equals(latestScheduledTaskId, that.latestScheduledTaskId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        scheduleStatus,
        latestScheduledTaskId,
        byteAwaitingCompaction,
        byteProcessed,
        segmentCountAwaitingCompaction,
        segmentCountProcessed,
        intervalCountAwaitingCompaction,
        intervalCountProcessed
    );
  }
}
