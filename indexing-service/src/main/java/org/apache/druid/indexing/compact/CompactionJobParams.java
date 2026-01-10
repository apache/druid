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

package org.apache.druid.indexing.compact;

import org.apache.druid.segment.metadata.CompactionFingerprintMapper;
import org.apache.druid.segment.metadata.CompactionStateStorage;
import org.apache.druid.server.compaction.CompactionSnapshotBuilder;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;

/**
 * Parameters used while creating a {@link CompactionJob} using a {@link CompactionJobTemplate}.
 */
public class CompactionJobParams
{
  private final DateTime scheduleStartTime;
  private final TimelineProvider timelineProvider;
  private final ClusterCompactionConfig clusterCompactionConfig;
  private final CompactionSnapshotBuilder snapshotBuilder;
  private final CompactionFingerprintMapper fingerprintMapper;
  private final CompactionStateStorage compactionStateStorage;

  public CompactionJobParams(
      DateTime scheduleStartTime,
      ClusterCompactionConfig clusterCompactionConfig,
      TimelineProvider timelineProvider,
      CompactionSnapshotBuilder snapshotBuilder,
      CompactionFingerprintMapper fingerprintMapper,
      CompactionStateStorage compactionStateStorage
  )
  {
    this.scheduleStartTime = scheduleStartTime;
    this.clusterCompactionConfig = clusterCompactionConfig;
    this.timelineProvider = timelineProvider;
    this.snapshotBuilder = snapshotBuilder;
    this.fingerprintMapper = fingerprintMapper;
    this.compactionStateStorage = compactionStateStorage;
  }

  /**
   * Timestamp denoting the start of the current run of the scheduler which has
   * triggered creation of jobs using these {@link CompactionJobParams}.
   */
  public DateTime getScheduleStartTime()
  {
    return scheduleStartTime;
  }

  /**
   * Cluster-level compaction config containing details such as the engine,
   * compaction search policy, etc. to use while creating {@link CompactionJob}.
   */
  public ClusterCompactionConfig getClusterCompactionConfig()
  {
    return clusterCompactionConfig;
  }

  /**
   * Provides the full {@link SegmentTimeline} of used segments for the given
   * datasource. This timeline is used to identify eligible intervals for which
   * compaction jobs should be created.
   */
  public SegmentTimeline getTimeline(String dataSource)
  {
    return timelineProvider.getTimelineForDataSource(dataSource);
  }

  /**
   * Used to build an {@link org.apache.druid.server.coordinator.AutoCompactionSnapshot}
   * for all the datasources at the end of the current run. During the run, as
   * candidate intervals are identified as compacted, skipped or pending, they
   * should be updated in this snapshot builder by invoking
   * {@link CompactionSnapshotBuilder#addToComplete}, {@link CompactionSnapshotBuilder#addToSkipped}
   * and {@link CompactionSnapshotBuilder#addToPending} respectively.
   */
  public CompactionSnapshotBuilder getSnapshotBuilder()
  {
    return snapshotBuilder;
  }

  public CompactionFingerprintMapper getFingerprintMapper()
  {
    return fingerprintMapper;
  }

  public CompactionStateStorage getCompactionStateStorageImpl()
  {
    return compactionStateStorage;
  }

  @FunctionalInterface
  public interface TimelineProvider
  {
    SegmentTimeline getTimelineForDataSource(String dataSource);
  }
}
