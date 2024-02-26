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

package org.apache.druid.indexing.rabbitstream.supervisor;

import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class RabbitStreamSupervisorReportPayload extends SeekableStreamSupervisorReportPayload<String, Long>
{
  public RabbitStreamSupervisorReportPayload(
      String dataSource,
      String stream,
      int partitions,
      int replicas,
      long durationSeconds,
      @Nullable Map<String, Long> latestOffsets,
      @Nullable Map<String, Long> minimumLag,
      @Nullable Long aggregateLag,
      @Nullable DateTime offsetsLastUpdated,
      boolean suspended,
      boolean healthy,
      SupervisorStateManager.State state,
      SupervisorStateManager.State detailedState,
      List<SupervisorStateManager.ExceptionEvent> recentErrors)
  {
    super(
        dataSource,
        stream,
        partitions,
        replicas,
        durationSeconds,
        latestOffsets,
        minimumLag,
        aggregateLag,
        null,
        null,
        offsetsLastUpdated,
        suspended,
        healthy,
        state,
        detailedState,
        recentErrors);
  }

  @Override
  public String toString()
  {
    return "RabbitStreamSupervisorReportPayload{" +
        "dataSource='" + getDataSource() + '\'' +
        ", stream='" + getStream() + '\'' +
        ", partitions=" + getPartitions() +
        ", replicas=" + getReplicas() +
        ", durationSeconds=" + getDurationSeconds() +
        ", active=" + getActiveTasks() +
        ", publishing=" + getPublishingTasks() +
        (getLatestOffsets() != null ? ", latestOffsets=" + getLatestOffsets() : "") +
        (getMinimumLag() != null ? ", minimumLag=" + getMinimumLag() : "") +
        (getAggregateLag() != null ? ", aggregateLag=" + getAggregateLag() : "") +
        (getOffsetsLastUpdated() != null ? ", sequenceLastUpdated=" + getOffsetsLastUpdated() : "") +
        ", suspended=" + isSuspended() +
        ", healthy=" + isHealthy() +
        ", state=" + getState() +
        ", detailedState=" + getDetailedState() +
        ", recentErrors=" + getRecentErrors() +
        '}';
  }
}
