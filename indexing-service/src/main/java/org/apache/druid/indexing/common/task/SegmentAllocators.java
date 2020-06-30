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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.SupervisorTaskAccess;
import org.apache.druid.indexing.common.task.batch.partition.CompletePartitionAnalysis;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;

import javax.annotation.Nullable;
import java.io.IOException;

public final class SegmentAllocators
{
  /**
   * Creates a new {@link SegmentAllocator} for the linear partitioning.
   * supervisorTaskAccess can be null if this method is called by the {@link IndexTask}.
   */
  public static SegmentAllocatorForBatch forLinearPartitioning(
      final TaskToolbox toolbox,
      final String taskId,
      final @Nullable SupervisorTaskAccess supervisorTaskAccess,
      final DataSchema dataSchema,
      final TaskLockHelper taskLockHelper,
      final boolean appendToExisting,
      final PartitionsSpec partitionsSpec
  ) throws IOException
  {
    if (appendToExisting || taskLockHelper.isUseSegmentLock()) {
      return new OverlordCoordinatingSegmentAllocator(
          toolbox,
          taskId,
          supervisorTaskAccess,
          dataSchema,
          taskLockHelper,
          appendToExisting,
          partitionsSpec
      );
    } else {
      if (supervisorTaskAccess == null) {
        return new LocalSegmentAllocator(
            toolbox,
            taskId,
            dataSchema.getDataSource(),
            dataSchema.getGranularitySpec()
        );
      } else {
        return new SupervisorTaskCoordinatingSegmentAllocator(
            supervisorTaskAccess.getSupervisorTaskId(),
            taskId,
            supervisorTaskAccess.getTaskClient()
        );
      }
    }
  }

  /**
   * Creates a new {@link SegmentAllocator} for the hash and range partitioning.
   * supervisorTaskAccess can be null if this method is called by the {@link IndexTask}.
   */
  public static SegmentAllocatorForBatch forNonLinearPartitioning(
      final TaskToolbox toolbox,
      final String dataSource,
      final String taskId,
      final GranularitySpec granularitySpec,
      final @Nullable SupervisorTaskAccess supervisorTaskAccess,
      final CompletePartitionAnalysis partitionAnalysis
  ) throws IOException
  {
    return new CachingLocalSegmentAllocator(
        toolbox,
        dataSource,
        taskId,
        granularitySpec,
        supervisorTaskAccess,
        partitionAnalysis
    );
  }

  private SegmentAllocators()
  {
  }
}
