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

package org.apache.druid.indexing.worker.shuffle;

import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.DeepStoragePartitionStat;
import org.apache.druid.indexing.common.task.batch.parallel.GenericPartitionStat;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class DeepStorageIntermediaryDataManager implements IntermediaryDataManager
{
  public static final String SHUFFLE_DATA_DIR_PREFIX = "shuffle-data";
  private final DataSegmentPusher dataSegmentPusher;

  @Override
  public DataSegment addSegment(String supervisorTaskId, String subTaskId, DataSegment segment, File segmentDir)
      throws IOException
  {
    final BucketNumberedShardSpec<?> bucketNumberedShardSpec = (BucketNumberedShardSpec<?>) segment.getShardSpec();
    final String partitionFilePath = getPartitionFilePath(
        supervisorTaskId,
        subTaskId,
        segment.getInterval(),
        bucketNumberedShardSpec.getBucketId() // we must use the bucket ID instead of partition ID
    );
    return dataSegmentPusher.pushToPath(segmentDir, segment, SHUFFLE_DATA_DIR_PREFIX + "/" + partitionFilePath);
  }

  @Inject
  public DeepStorageIntermediaryDataManager(DataSegmentPusher dataSegmentPusher)
  {
    this.dataSegmentPusher = dataSegmentPusher;
  }

  @Override
  public GenericPartitionStat generatePartitionStat(TaskToolbox toolbox, DataSegment segment)
  {
    return new DeepStoragePartitionStat(
        toolbox.getTaskExecutorNode().getHost(),
        toolbox.getTaskExecutorNode().getPortToUse(),
        toolbox.getTaskExecutorNode().isEnableTlsPort(),
        segment.getInterval(),
        (BucketNumberedShardSpec) segment.getShardSpec(),
        null, // numRows is not supported yet
        null, // sizeBytes is not supported yet
        segment.getLoadSpec()
    );
  }

  @Nullable
  @Override
  public Optional<ByteSource> findPartitionFile(String supervisorTaskId, String subTaskId, Interval interval, int bucketId)
  {
    return Optional.empty();
  }

  @Override
  public void deletePartitions(String supervisorTaskId)
  {
    throw new UnsupportedOperationException("Clean up will be taken care by a coordinator duty");
  }
}
