/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.druid.indexer.TaskReport;
import io.druid.indexer.TaskState;

import javax.annotation.Nullable;

/**
 * Should be synced with io.druid.indexing.common.TaskStatus
 */
public class TaskStatus
{
  private final String id;
  private final TaskState status;
  private final TaskReport report;
  private final long duration;

  @JsonCreator
  public TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") TaskState status,
      @JsonProperty("report") @Nullable TaskReport report,
      @JsonProperty("duration") long duration
  )
  {
    this.id = id;
    this.status = status;
    this.report = report;
    this.duration = duration;

    // Check class invariants.
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkNotNull(status, "status");
  }

  @JsonProperty("id")
  public String getId()
  {
    return id;
  }

  @JsonProperty("status")
  public TaskState getStatusCode()
  {
    return status;
  }

  @JsonProperty("report")
  public TaskReport getReport()
  {
    return report;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
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
    TaskStatus that = (TaskStatus) o;
    return duration == that.duration &&
           java.util.Objects.equals(id, that.id) &&
           status == that.status &&
           java.util.Objects.equals(report, that.report);
  }

  @Override
  public int hashCode()
  {
    return java.util.Objects.hash(id, status, report, duration);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("report", report)
                  .add("duration", duration)
                  .toString();
  }
}
