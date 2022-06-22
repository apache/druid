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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.TaskLockHelper.OverwritingRootGenerationPartitions;
import org.apache.druid.indexing.common.task.batch.parallel.SupervisorTaskAccess;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwritePartialShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Segment allocator which allocates new segments using the overlord per request.
 */
public class OverlordCoordinatingSegmentAllocator implements SegmentAllocatorForBatch
{
  private final ActionBasedSegmentAllocator internalAllocator;
  private final LinearlyPartitionedSequenceNameFunction sequenceNameFunction;

  OverlordCoordinatingSegmentAllocator(
      final TaskToolbox toolbox,
      final String taskId,
      final @Nullable SupervisorTaskAccess supervisorTaskAccess,
      final DataSchema dataSchema,
      final TaskLockHelper taskLockHelper,
      final AbstractTask.IngestionMode ingestionMode,
      final PartitionsSpec partitionsSpec
  )
  {
    final TaskActionClient taskActionClient =
        supervisorTaskAccess == null
        ? toolbox.getTaskActionClient()
        : new SurrogateTaskActionClient(supervisorTaskAccess.getSupervisorTaskId(), toolbox.getTaskActionClient());
    this.internalAllocator = new ActionBasedSegmentAllocator(
        taskActionClient,
        dataSchema,
        (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
          final GranularitySpec granularitySpec = schema.getGranularitySpec();
          final Interval interval = granularitySpec
              .bucketInterval(row.getTimestamp())
              .or(granularitySpec.getSegmentGranularity().bucket(row.getTimestamp()));
          final PartialShardSpec partialShardSpec = createPartialShardSpec(
              ingestionMode,
              partitionsSpec,
              taskLockHelper,
              interval
          );
          return new SegmentAllocateAction(
              schema.getDataSource(),
              row.getTimestamp(),
              schema.getGranularitySpec().getQueryGranularity(),
              schema.getGranularitySpec().getSegmentGranularity(),
              sequenceName,
              previousSegmentId,
              skipSegmentLineageCheck,
              partialShardSpec,
              taskLockHelper.getLockGranularityToUse(),
              taskLockHelper.getLockTypeToUse()
          );
        }
    );
    this.sequenceNameFunction = new LinearlyPartitionedSequenceNameFunction(taskId);
  }

  @Nullable
  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return internalAllocator.allocate(row, sequenceName, previousSegmentId, skipSegmentLineageCheck);
  }

  private static PartialShardSpec createPartialShardSpec(
      AbstractTask.IngestionMode ingestionMode,
      PartitionsSpec partitionsSpec,
      TaskLockHelper taskLockHelper,
      Interval interval
  )
  {
    if (partitionsSpec.getType() == SecondaryPartitionType.LINEAR) {
      if (taskLockHelper.isUseSegmentLock()) {
        if (taskLockHelper.hasOverwritingRootGenerationPartition(interval) && (ingestionMode
                                                                               != AbstractTask.IngestionMode.APPEND)) {
          final OverwritingRootGenerationPartitions overwritingRootGenerationPartitions = taskLockHelper
              .getOverwritingRootGenerationPartition(interval);
          if (overwritingRootGenerationPartitions == null) {
            throw new ISE("Can't find overwritingSegmentMeta for interval[%s]", interval);
          }
          return new NumberedOverwritePartialShardSpec(
              overwritingRootGenerationPartitions.getStartRootPartitionId(),
              overwritingRootGenerationPartitions.getEndRootPartitionId(),
              overwritingRootGenerationPartitions.getMinorVersionForNewSegments()
          );
        }
      }
      return NumberedPartialShardSpec.instance();
    } else {
      throw new ISE(
          "%s is not supported for partitionsSpec[%s]",
          OverlordCoordinatingSegmentAllocator.class.getName(),
          partitionsSpec.getClass().getName()
      );
    }
  }

  @Override
  public SequenceNameFunction getSequenceNameFunction()
  {
    return sequenceNameFunction;
  }
}
