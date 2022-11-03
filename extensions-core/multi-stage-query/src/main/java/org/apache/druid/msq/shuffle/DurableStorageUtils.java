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

package org.apache.druid.msq.shuffle;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Helper class that fetches the directory and file names corresponding to file location
 */
public class DurableStorageUtils
{
  public static final String SUCCESS_MARKER_FILENAME = "__success";

  public static String getControllerDirectory(final String controllerTaskId)
  {
    return StringUtils.format("controller_%s", IdUtils.validateId("controller task ID", controllerTaskId));
  }

  public static String getSuccessFilePath(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber
  )
  {
    String folderName = getWorkerOutputFolderName(
        controllerTaskId,
        stageNumber,
        workerNumber
    );
    String fileName = StringUtils.format("%s/%s", folderName, SUCCESS_MARKER_FILENAME);
    return fileName;
  }

  /**
   * Fetches the directory location where workers will store the partition files corresponding to the stage number
   */
  public static String getWorkerOutputFolderName(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber
  )
  {
    return StringUtils.format(
        "%s/stage_%d/worker_%d",
        getControllerDirectory(controllerTaskId),
        stageNumber,
        workerNumber
    );
  }

  /**
   * Fetches the directory location where a particular worker will store the partition files corresponding to the
   * stage number, and it's task id
   */
  public static String getTaskIdOutputsFolderName(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber,
      final String taskId
  )
  {
    return StringUtils.format(
        "%s/taskId_%s",
        getWorkerOutputFolderName(controllerTaskId, stageNumber, workerNumber),
        taskId
    );
  }

  /**
   * Fetches the file location where a particular worker writes the data corresponding to a particular stage
   * and partition
   */
  public static String getPartitionOutputsFileNameForPartition(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber,
      final String taskId,
      final int partitionNumber
  )
  {
    return StringUtils.format(
        "%s/part_%d",
        getTaskIdOutputsFolderName(controllerTaskId, stageNumber, workerNumber, taskId),
        partitionNumber
    );
  }
}
