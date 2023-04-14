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
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

  private final File[] baseTaskDirs;
  // Initialize to a negative number because it ensures that we can handle the overflow-rollover case
  private final AtomicInteger iterationCounter = new AtomicInteger(Integer.MIN_VALUE);

  public TaskStorageDirTracker(List<File> baseTaskDirs)
  {
    this.baseTaskDirs = baseTaskDirs.toArray(new File[0]);
  }

  @LifecycleStart
  public void ensureDirectories()
  {
    for (File baseTaskDir : baseTaskDirs) {
      if (!baseTaskDir.exists()) {
        try {
          FileUtils.mkdirp(baseTaskDir);
        }
        catch (IOException e) {
          throw new ISE(
              e,
              "base task directory [%s] likely does not exist, please ensure it exists and the user has permissions.",
              baseTaskDir
          );
        }
      }
    }
  }

  public File pickBaseDir(String taskId)
  {
    if (baseTaskDirs.length == 1) {
      return baseTaskDirs[0];
    }

    // if the task directory already exists, we want to give it precedence, so check.
    for (File baseTaskDir : baseTaskDirs) {
      if (new File(baseTaskDir, taskId).exists()) {
        return baseTaskDir;
      }
    }

    // if it doesn't exist, pick one round-robin and return.  This can be negative, so abs() it
    final int currIncrement = Math.abs(iterationCounter.getAndIncrement() % baseTaskDirs.length);
    return baseTaskDirs[currIncrement % baseTaskDirs.length];
  }

  @Nullable
  public File findExistingTaskDir(String taskId)
  {
    if (baseTaskDirs.length == 1) {
      return new File(baseTaskDirs[0], taskId);
    }

    for (File baseTaskDir : baseTaskDirs) {
      final File candidateLocation = new File(baseTaskDir, taskId);
      if (candidateLocation.exists()) {
        return candidateLocation;
      }
    }
    return null;
  }
}
