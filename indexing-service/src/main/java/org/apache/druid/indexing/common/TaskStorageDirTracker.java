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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

public class TaskStorageDirTracker
{
  public static TaskStorageDirTracker fromConfigs(WorkerConfig workerConfig, TaskConfig taskConfig)
  {
    if (workerConfig == null) {
      return new TaskStorageDirTracker(ImmutableList.of(taskConfig.getBaseTaskDir()));
    } else {
      final List<String> basePaths = workerConfig.getBaseTaskDirs();
      if (basePaths == null) {
        return new TaskStorageDirTracker(ImmutableList.of(taskConfig.getBaseTaskDir()));
      }
      return new TaskStorageDirTracker(
          basePaths.stream().map(File::new).collect(Collectors.toList())
      );
    }
  }

  private final List<File> baseTaskDirs;

  public TaskStorageDirTracker(List<File> baseTaskDirs)
  {
    this.baseTaskDirs = baseTaskDirs;
  }

  public File pickBaseDir(String taskId) throws IOException
  {
    File leastUsed = null;
    long numEntries = Long.MAX_VALUE;

    for (File baseTaskDir : baseTaskDirs) {
      if (new File(baseTaskDir, taskId).exists()) {
        return baseTaskDir;
      }

      long numFiles = Files.list(baseTaskDir.toPath()).count();
      if (numFiles < numEntries) {
        numEntries = numFiles;
        leastUsed = baseTaskDir;
      }
    }
    return leastUsed;
  }

  public File findExistingTaskDir(String taskId)
  {
    for (File baseTaskDir : baseTaskDirs) {
      final File candidateLocation = new File(baseTaskDir, taskId);
      if (candidateLocation.exists()) {
        return candidateLocation;
      }
    }
    return null;
  }
}
