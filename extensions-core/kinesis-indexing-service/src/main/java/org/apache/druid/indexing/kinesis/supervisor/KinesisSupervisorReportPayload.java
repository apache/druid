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

package org.apache.druid.indexing.kinesis.supervisor;

import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class KinesisSupervisorReportPayload extends SeekableStreamSupervisorReportPayload<String, String>
{
  public KinesisSupervisorReportPayload(
      String dataSource,
      String stream,
      Integer partitions,
      Integer replicas,
      Long durationSeconds,
      boolean suspended,
      boolean healthy,
      SupervisorStateManager.State state,
      SupervisorStateManager.State detailedState,
      List<SupervisorStateManager.ExceptionEvent> recentErrors,
      @Nullable Map<String, Long> minimumLagMillis,
      @Nullable Long aggregateLagMillis
  )
  {
    super(
        dataSource,
        stream,
        partitions,
        replicas,
        durationSeconds,
        null,
        null,
        null,
        minimumLagMillis,
        aggregateLagMillis,
        null,
        suspended,
        healthy,
        state,
        detailedState,
        recentErrors
    );
  }

  @Override
  public String toString()
  {
    return "KinesisSupervisorReportPayload{" +
           "dataSource='" + getDataSource() + '\'' +
           ", stream='" + getStream() + '\'' +
           ", partitions=" + getPartitions() +
           ", replicas=" + getReplicas() +
           ", durationSeconds=" + getDurationSeconds() +
           ", active=" + getActiveTasks() +
           ", publishing=" + getPublishingTasks() +
           ", suspended=" + isSuspended() +
           ", healthy=" + isHealthy() +
           ", state=" + getState() +
           ", detailedState=" + getDetailedState() +
           ", recentErrors=" + getRecentErrors() +
           (getMinimumLagMillis() != null ? ", minimumLagMillis=" + getMinimumLagMillis() : "") +
           (getAggregateLagMillis() != null ? ", aggregateLagMillis=" + getAggregateLagMillis() : "") +
           '}';
  }
}
