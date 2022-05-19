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
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.PartitionStat;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * This interface manages intermediary segments for data shuffle between native parallel index tasks.
 * In native parallel indexing, phase 1 tasks store segment files using the implementation of this interface
 * and phase 2 tasks read those files using {@link ShuffleClient}.
 *
 * This interface provides methods to store, find, and remove segment files.
 * Note that the implementation should also implement a self-cleanup mechanism to clean up stale segment files for
 * supervisorTask that is not running anymore.
 *
 * Extension can implement this interface to store intermediary data at custom location such as various cloud storages.
 */
@ExtensionPoint
public interface IntermediaryDataManager
{
  void start();

  void stop();

  /**
   * Write a segment into one of configured locations
   *
   * @param supervisorTaskId - Id of the supervisor task writing the segment
   * @param subTaskId - Id of the sub task writing the segment
   * @param segment - Segment to write
   * @param segmentDir - Directory of the segment to write
   *
   * @return the writen segment
   */
  DataSegment addSegment(String supervisorTaskId, String subTaskId, DataSegment segment, File segmentDir) throws IOException;

  /**
   * Find the partition file. Note that the returned ByteSource method size() should be fast.
   *
   * @param supervisorTaskId - Supervisor task id of the partition file to find
   * @param subTaskId - Sub task id of the partition file to find
   * @param interval - Interval of the partition file to find
   * @param bucketId - Bucket id of the partition file to find
   *
   * @return ByteSource wrapped in {@link Optional} if the file is found, otherwise return {@link Optional#empty()}
   */
  Optional<ByteSource> findPartitionFile(String supervisorTaskId, String subTaskId, Interval interval, int bucketId);

  /**
   * Delete the partitions
   *
   * @param supervisorTaskId - Supervisor task id of the partitions to delete
   *
   */
  void deletePartitions(String supervisorTaskId) throws IOException;

  PartitionStat generatePartitionStat(TaskToolbox toolbox, DataSegment segment);

  default String getPartitionFilePath(
      String supervisorTaskId,
      String subTaskId,
      Interval interval,
      int bucketId
  )
  {
    return Paths.get(getPartitionDirPath(supervisorTaskId, interval, bucketId), subTaskId).toString();
  }

  default String getPartitionDirPath(
      String supervisorTaskId,
      Interval interval,
      int bucketId
  )
  {
    return Paths.get(
        supervisorTaskId,
        interval.getStart().toString(),
        interval.getEnd().toString(),
        String.valueOf(bucketId)
    ).toString();
  }
}
