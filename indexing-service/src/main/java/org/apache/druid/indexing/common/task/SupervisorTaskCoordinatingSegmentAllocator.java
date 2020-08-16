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
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClient;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

import java.io.IOException;

/**
 * Segment allocator that allocates new segments using the supervisor task per request.
 */
public class SupervisorTaskCoordinatingSegmentAllocator implements SegmentAllocatorForBatch
{
  private final String supervisorTaskId;
  private final ParallelIndexSupervisorTaskClient taskClient;
  private final SequenceNameFunction sequenceNameFunction;

  SupervisorTaskCoordinatingSegmentAllocator(
      String supervisorTaskId,
      String taskId,
      ParallelIndexSupervisorTaskClient taskClient
  )
  {
    this.supervisorTaskId = supervisorTaskId;
    this.taskClient = taskClient;
    this.sequenceNameFunction = new LinearlyPartitionedSequenceNameFunction(taskId);
  }

  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return taskClient.allocateSegment(supervisorTaskId, row.getTimestamp());
  }

  @Override
  public SequenceNameFunction getSequenceNameFunction()
  {
    return sequenceNameFunction;
  }
}
