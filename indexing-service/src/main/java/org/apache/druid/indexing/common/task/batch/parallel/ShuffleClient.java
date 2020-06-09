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

import java.io.File;
import java.io.IOException;

/**
 * An interface for intermediate data shuffle during the parallel indexing.
 * The only available implementation for production code is {@link HttpShuffleClient} and
 * this interface is more for easier testing.
 *
 * @see org.apache.druid.indexing.worker.IntermediaryDataManager
 * @see PartialSegmentMergeTask
 */
public interface ShuffleClient
{
  /**
   * Fetch the segment file into the local storage for the given supervisorTaskId and the location.
   * If the segment file should be fetched from a remote site, the returned file will be created under the given
   * partitionDir. Otherwise, the returned file can be located in any path.
   */
  <T, P extends PartitionLocation<T>> File fetchSegmentFile(File partitionDir, String supervisorTaskId, P location)
      throws IOException;
}
