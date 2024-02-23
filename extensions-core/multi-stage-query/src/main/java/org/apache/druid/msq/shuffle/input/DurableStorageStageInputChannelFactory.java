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

package org.apache.druid.msq.shuffle.input;

import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.storage.StorageConnector;

import java.util.concurrent.ExecutorService;

/**
 * Used for reading stage results when the output of each stage is written out to durable storage.
 * If the user want's to read the output of a select query, please use {@link DurableStorageQueryResultsInputChannelFactory}
 */
public class DurableStorageStageInputChannelFactory extends DurableStorageInputChannelFactory
{
  public DurableStorageStageInputChannelFactory(
      String controllerTaskId,
      StorageConnector storageConnector,
      ExecutorService remoteInputStreamPool
  )
  {
    super(controllerTaskId, storageConnector, remoteInputStreamPool);
  }

  @Override
  public String getPartitionOutputsFileNameWithPathForPartition(
      String controllerTaskId,
      int stageNumber,
      int workerNo,
      int partitionNumber,
      String successfulTaskId
  )
  {
    return DurableStorageUtils.getPartitionOutputsFileNameWithPathForPartition(
        controllerTaskId,
        stageNumber,
        workerNo,
        successfulTaskId,
        partitionNumber
    );
  }

  @Override
  public String getWorkerOutputSuccessFilePath(String controllerTaskId, int stageNumber, int workerNo)
  {
    return DurableStorageUtils.getWorkerOutputSuccessFilePath(controllerTaskId, stageNumber, workerNo);
  }
}
