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

package org.apache.druid.frame.util;

import com.google.common.base.Splitter;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Helper class that fetches the directory and file names corresponding to file location
 */
public class DurableStorageUtils
{
  public static final String SUCCESS_MARKER_FILENAME = "__success";
  public static final Splitter SPLITTER = Splitter.on("/").limit(2);

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
    return StringUtils.format("%s/%s", folderName, SUCCESS_MARKER_FILENAME);
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

  /**
   * Fetches the file location where a particular worker writes the data corresponding to a particular stage
   * and a custom path name
   */
  public static String getOutputsFileNameForPath(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber,
      final String taskId,
      final String path
  )
  {
    return StringUtils.format(
        "%s/%s",
        getTaskIdOutputsFolderName(controllerTaskId, stageNumber, workerNumber, taskId),
        path
    );
  }

  /**
   * Tries to parse out the controller taskID from the input path.
   * <br></br>
   * For eg:
   * <br/>
   * <ul>
   *   <li>for input path <b>controller_query_id/task/123</b> the function will return <b>controller_query_id</b></li>
   *   <li>for input path <b>abcd</b>, the function will return <b>abcd</b></li>
   *   <li>for input path <b>null</b>, the function will return <b>null</b></li>
   * </ul>
   */
  @Nullable
  public static String getControllerTaskIdWithPrefixFromPath(String path)
  {
    if (path == null) {
      return null;
    }
    Iterator<String> elements = SPLITTER.split(path).iterator();
    if (elements.hasNext()) {
      return elements.next();
    } else {
      return null;
    }
  }
}
