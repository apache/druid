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
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.task.AbstractTask.OverwritingSegmentMeta;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwritingShardSpecFactory;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

public class RemoteSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final String taskId;
  private final ActionBasedSegmentAllocator internalAllocator;

  public RemoteSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      DataSchema dataSchema,
      boolean isOverwriteMode,
      boolean isChangeSegmentGranularity,
      Map<Interval, OverwritingSegmentMeta> overwritingSegmentMetaMap
  )
  {
    this.taskId = taskId;
    this.internalAllocator = new ActionBasedSegmentAllocator(
        toolbox.getTaskActionClient(),
        dataSchema,
        (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
          final GranularitySpec granularitySpec = schema.getGranularitySpec();
          final Interval interval = granularitySpec
              .bucketInterval(row.getTimestamp())
              .or(granularitySpec.getSegmentGranularity().bucket(row.getTimestamp()));
          final ShardSpecFactory shardSpecFactory;
          if (isOverwriteMode && !isChangeSegmentGranularity) {
            final OverwritingSegmentMeta overwritingSegmentMeta = overwritingSegmentMetaMap.get(interval);
            if (overwritingSegmentMeta == null) {
              throw new ISE("Can't find overwritingSegmentMeta for interval[%s]", interval);
            }
            shardSpecFactory = new NumberedOverwritingShardSpecFactory(
                overwritingSegmentMeta.getStartRootPartitionId(),
                overwritingSegmentMeta.getEndRootPartitionId(),
                overwritingSegmentMeta.getMinorVersionForNewSegments()
            );
          } else {
            shardSpecFactory = NumberedShardSpecFactory.instance();
          }
          return new SegmentAllocateAction(
              schema.getDataSource(),
              row.getTimestamp(),
              schema.getGranularitySpec().getQueryGranularity(),
              schema.getGranularitySpec().getSegmentGranularity(),
              sequenceName,
              previousSegmentId,
              skipSegmentLineageCheck,
              shardSpecFactory
          );
        }
    );
  }

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

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    return taskId;
  }
}
