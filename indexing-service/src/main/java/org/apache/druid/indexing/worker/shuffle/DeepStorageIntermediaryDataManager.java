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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.loading.DataSegmentKiller;
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
  private final DataSegmentKiller dataSegmentKiller;

  /**
   * Deep storage path to the directory that holds all shuffle intermediate files for {@code supervisorTaskId},
   * relative to the deep storage root configured for {@link DataSegmentPusher} (for example
   * {@code druid.storage.storageDirectory} for local and HDFS). Matches the prefix used by {@link #addSegment}.
   */
  public static String retrieveShuffleDataStoragePath(String supervisorTaskId)
  {
    return SHUFFLE_DATA_DIR_PREFIX + "/" + supervisorTaskId;
  }

  /**
   * Used by Guice to create an instance of {@link DeepStorageIntermediaryDataManager}.
   *
   * @param dataSegmentPusher Always non-null
   * @param dataSegmentKiller Can be null in certain cases such as on MiddleManagers
   *                          when using druid.storage.type=s3 since the respective
   *                          S3DataSegmentKiller uses scheme "s3_zip" instead of "s3"
   */
  @Inject
  public DeepStorageIntermediaryDataManager(
      DataSegmentPusher dataSegmentPusher,
      @Nullable DataSegmentKiller dataSegmentKiller
  )
  {
    this.dataSegmentPusher = dataSegmentPusher;
    this.dataSegmentKiller = dataSegmentKiller;
  }

  @Override
  public void start()
  {
    // nothing
  }

  @Override
  public void stop()
  {
    // nothing
  }

  @Override
  public DataSegment addSegment(String supervisorTaskId, String subTaskId, DataSegment segment, File segmentDir)
      throws IOException
  {
    if (!(segment.getShardSpec() instanceof BucketNumberedShardSpec)) {
      throw new IAE(
          "Invalid shardSpec type. Expected [%s] but got [%s]",
          BucketNumberedShardSpec.class.getName(),
          segment.getShardSpec().getClass().getName()
      );
    }
    final BucketNumberedShardSpec<?> bucketNumberedShardSpec = (BucketNumberedShardSpec<?>) segment.getShardSpec();
    final String partitionFilePath = getPartitionFilePath(
        supervisorTaskId,
        subTaskId,
        segment.getInterval(),
        bucketNumberedShardSpec.getBucketId() // we must use the bucket ID instead of partition ID
    );
    return dataSegmentPusher.pushToPath(segmentDir, segment, SHUFFLE_DATA_DIR_PREFIX + "/" + partitionFilePath);
  }

  @Override
  public DeepStoragePartitionStat generatePartitionStat(TaskToolbox toolbox, DataSegment segment)
  {
    return new DeepStoragePartitionStat(
        segment.getInterval(),
        (BucketNumberedShardSpec) segment.getShardSpec(),
        segment.getLoadSpec()
    );
  }

  @Nullable
  @Override
  public Optional<ByteSource> findPartitionFile(String supervisorTaskId, String subTaskId, Interval interval, int bucketId)
  {
    throw new UnsupportedOperationException("Not supported, get partition file using segment loadspec");
  }

  @Override
  public void deletePartitions(String supervisorTaskId) throws IOException
  {
    if (dataSegmentKiller == null) {
      throw new ISE("No instance was bound for the DataSegmentKiller");
    } else {
      dataSegmentKiller.killRecursively(retrieveShuffleDataStoragePath(supervisorTaskId));
    }
  }
}
