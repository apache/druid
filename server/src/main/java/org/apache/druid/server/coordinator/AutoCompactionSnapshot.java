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
import org.apache.druid.java.util.common.ISE;

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
  private long bytesAwaitingCompaction;
  @JsonProperty
  private long bytesCompacted;
  @JsonProperty
  private long bytesSkipped;
  @JsonProperty
  private long segmentCountAwaitingCompaction;
  @JsonProperty
  private long segmentCountCompacted;
  @JsonProperty
  private long segmentCountSkipped;
  @JsonProperty
  private long intervalCountAwaitingCompaction;
  @JsonProperty
  private long intervalCountCompacted;
  @JsonProperty
  private long intervalCountSkipped;

  @JsonCreator
  public AutoCompactionSnapshot(
      @JsonProperty @NotNull String dataSource,
      @JsonProperty @NotNull AutoCompactionScheduleStatus scheduleStatus,
      @JsonProperty long bytesAwaitingCompaction,
      @JsonProperty long bytesCompacted,
      @JsonProperty long bytesSkipped,
      @JsonProperty long segmentCountAwaitingCompaction,
      @JsonProperty long segmentCountCompacted,
      @JsonProperty long segmentCountSkipped,
      @JsonProperty long intervalCountAwaitingCompaction,
      @JsonProperty long intervalCountCompacted,
      @JsonProperty long intervalCountSkipped
  )
  {
    this.dataSource = dataSource;
    this.scheduleStatus = scheduleStatus;
    this.bytesAwaitingCompaction = bytesAwaitingCompaction;
    this.bytesCompacted = bytesCompacted;
    this.bytesSkipped = bytesSkipped;
    this.segmentCountAwaitingCompaction = segmentCountAwaitingCompaction;
    this.segmentCountCompacted = segmentCountCompacted;
    this.segmentCountSkipped = segmentCountSkipped;
    this.intervalCountAwaitingCompaction = intervalCountAwaitingCompaction;
    this.intervalCountCompacted = intervalCountCompacted;
    this.intervalCountSkipped = intervalCountSkipped;
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

  public long getBytesAwaitingCompaction()
  {
    return bytesAwaitingCompaction;
  }

  public long getBytesCompacted()
  {
    return bytesCompacted;
  }

  public long getBytesSkipped()
  {
    return bytesSkipped;
  }

  public long getSegmentCountAwaitingCompaction()
  {
    return segmentCountAwaitingCompaction;
  }

  public long getSegmentCountCompacted()
  {
    return segmentCountCompacted;
  }

  public long getSegmentCountSkipped()
  {
    return segmentCountSkipped;
  }

  public long getIntervalCountAwaitingCompaction()
  {
    return intervalCountAwaitingCompaction;
  }

  public long getIntervalCountCompacted()
  {
    return intervalCountCompacted;
  }

  public long getIntervalCountSkipped()
  {
    return intervalCountSkipped;
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
    return bytesAwaitingCompaction == that.bytesAwaitingCompaction &&
           bytesCompacted == that.bytesCompacted &&
           bytesSkipped == that.bytesSkipped &&
           segmentCountAwaitingCompaction == that.segmentCountAwaitingCompaction &&
           segmentCountCompacted == that.segmentCountCompacted &&
           segmentCountSkipped == that.segmentCountSkipped &&
           intervalCountAwaitingCompaction == that.intervalCountAwaitingCompaction &&
           intervalCountCompacted == that.intervalCountCompacted &&
           intervalCountSkipped == that.intervalCountSkipped &&
           dataSource.equals(that.dataSource) &&
           scheduleStatus == that.scheduleStatus;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        scheduleStatus,
        bytesAwaitingCompaction,
        bytesCompacted,
        bytesSkipped,
        segmentCountAwaitingCompaction,
        segmentCountCompacted,
        segmentCountSkipped,
        intervalCountAwaitingCompaction,
        intervalCountCompacted,
        intervalCountSkipped
    );
  }

  public static class Builder
  {
    private String dataSource;
    private AutoCompactionScheduleStatus scheduleStatus;
    private long bytesAwaitingCompaction;
    private long bytesCompacted;
    private long bytesSkipped;
    private long segmentCountAwaitingCompaction;
    private long segmentCountCompacted;
    private long segmentCountSkipped;
    private long intervalCountAwaitingCompaction;
    private long intervalCountCompacted;
    private long intervalCountSkipped;


    public Builder(
        @NotNull String dataSource,
        @NotNull AutoCompactionScheduleStatus scheduleStatus
    )
    {
      this.dataSource = dataSource;
      this.scheduleStatus = scheduleStatus;
      this.bytesAwaitingCompaction = 0;
      this.bytesCompacted = 0;
      this.bytesSkipped = 0;
      this.segmentCountAwaitingCompaction = 0;
      this.segmentCountCompacted = 0;
      this.segmentCountSkipped = 0;
      this.intervalCountAwaitingCompaction = 0;
      this.intervalCountCompacted = 0;
      this.intervalCountSkipped = 0;
    }

    public Builder incrementBytesAwaitingCompaction(long incrementValue)
    {
      this.bytesAwaitingCompaction = this.bytesAwaitingCompaction + incrementValue;
      return this;
    }

    public Builder incrementBytesCompacted(long incrementValue)
    {
      this.bytesCompacted = this.bytesCompacted + incrementValue;
      return this;
    }

    public Builder incrementSegmentCountAwaitingCompaction(long incrementValue)
    {
      this.segmentCountAwaitingCompaction = this.segmentCountAwaitingCompaction + incrementValue;
      return this;
    }

    public Builder incrementSegmentCountCompacted(long incrementValue)
    {
      this.segmentCountCompacted = this.segmentCountCompacted + incrementValue;
      return this;
    }

    public Builder incrementIntervalCountAwaitingCompaction(long incrementValue)
    {
      this.intervalCountAwaitingCompaction = this.intervalCountAwaitingCompaction + incrementValue;
      return this;
    }

    public Builder incrementIntervalCountCompacted(long incrementValue)
    {
      this.intervalCountCompacted = this.intervalCountCompacted + incrementValue;
      return this;
    }

    public Builder incrementBytesSkipped(long incrementValue)
    {
      this.bytesSkipped = this.bytesSkipped + incrementValue;
      return this;
    }

    public Builder incrementSegmentCountSkipped(long incrementValue)
    {
      this.segmentCountSkipped = this.segmentCountSkipped + incrementValue;
      return this;
    }

    public Builder incrementIntervalCountSkipped(long incrementValue)
    {
      this.intervalCountSkipped = this.intervalCountSkipped + incrementValue;
      return this;
    }

    public AutoCompactionSnapshot build()
    {
      if (dataSource == null || dataSource.isEmpty()) {
        throw new ISE("Invalid dataSource name");
      }
      if (scheduleStatus == null) {
        throw new ISE("scheduleStatus cannot be null");
      }
      return new AutoCompactionSnapshot(
          dataSource,
          scheduleStatus,
          bytesAwaitingCompaction,
          bytesCompacted,
          bytesSkipped,
          segmentCountAwaitingCompaction,
          segmentCountCompacted,
          segmentCountSkipped,
          intervalCountAwaitingCompaction,
          intervalCountCompacted,
          intervalCountSkipped
      );
    }
  }
}
