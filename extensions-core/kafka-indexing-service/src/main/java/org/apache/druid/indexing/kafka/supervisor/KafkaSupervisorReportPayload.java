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

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class KafkaSupervisorReportPayload extends SeekableStreamSupervisorReportPayload<Integer, Long>
{
  public KafkaSupervisorReportPayload(
      String dataSource,
      String topic,
      int partitions,
      int replicas,
      long durationSeconds,
      @Nullable Map<Integer, Long> latestOffsets,
      @Nullable Map<Integer, Long> minimumLag,
      @Nullable Long aggregateLag,
      @Nullable DateTime offsetsLastUpdated,
      boolean suspended,
      SeekableStreamSupervisorStateManager.State state,
      List<SeekableStreamSupervisorStateManager.State> stateHistory,
      List<SeekableStreamSupervisorStateManager.ExceptionEvent> recentErrors
  )
  {
    super(
        dataSource,
        topic,
        partitions,
        replicas,
        durationSeconds,
        latestOffsets,
        minimumLag,
        aggregateLag,
        offsetsLastUpdated,
        suspended,
        state,
        stateHistory,
        recentErrors
    );
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorReportPayload{" +
           "dataSource='" + getDataSource() + '\'' +
           ", topic='" + getStream() + '\'' +
           ", partitions=" + getPartitions() +
           ", replicas=" + getReplicas() +
           ", durationSeconds=" + getDurationSeconds() +
           ", active=" + getActiveTasks() +
           ", publishing=" + getPublishingTasks() +
           (getLatestOffsets() != null ? ", latestOffsets=" + getLatestOffsets() : "") +
           (getMinimumLag() != null ? ", minimumLag=" + getMinimumLag() : "") +
           (getAggregateLag() != null ? ", aggregateLag=" + getAggregateLag() : "") +
           (getOffsetsLastUpdated() != null ? ", sequenceLastUpdated=" + getOffsetsLastUpdated() : "") +
           ", suspended=" + getSuspended() +
           ", state=" + getState() +
           ", stateHistory=" + getStateHistory() +
           ", recentErrors=" + getRecentErrors() +
           '}';
  }
}
