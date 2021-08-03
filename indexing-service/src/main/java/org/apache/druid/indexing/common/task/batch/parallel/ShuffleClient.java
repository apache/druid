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

package org.apache.druid.indexing.common.task.batch.parallel;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;

import java.io.File;
import java.io.IOException;

/**
 * An interface for intermediate data shuffle during the parallel indexing.
 *
 * Extension can implement this interface to fetch intermediary data at custom location such as various cloud storages.
 *
 * @see IntermediaryDataManager
 * @see PartialSegmentMergeTask
 */
@ExtensionPoint
public interface ShuffleClient<P extends PartitionLocation>
{
  /**
   * Fetch the segment file into the local storage for the given supervisorTaskId and the location.
   * If the segment file should be fetched from a remote site, the returned file will be created under the given
   * partitionDir. Otherwise, the returned file can be located in any path.
   * @return dir containing the unzipped segment files
   */
  File fetchSegmentFile(File partitionDir, String supervisorTaskId, P location)
      throws IOException;
}
