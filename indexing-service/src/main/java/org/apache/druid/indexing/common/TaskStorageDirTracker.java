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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.ISE;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskStorageDirTracker
{
  private int taskDirIndex = 0;

  private final List<File> baseTaskDirs = new ArrayList<>();

  private final Map<String, File> taskToTempDirMap = new HashMap<>();

  @Inject
  public TaskStorageDirTracker(final TaskConfig taskConfig)
  {
    this(taskConfig.getBaseTaskDirPaths());
  }

  @VisibleForTesting
  public TaskStorageDirTracker(final List<String> baseTaskDirPaths)
  {
    for (String baseTaskDirPath : baseTaskDirPaths) {
      baseTaskDirs.add(new File(baseTaskDirPath));
    }
  }

  public File getTaskDir(String taskId)
  {
    return new File(getBaseTaskDir(taskId), taskId);
  }

  public File getTaskWorkDir(String taskId)
  {
    return new File(getTaskDir(taskId), "work");
  }

  public File getTaskTempDir(String taskId)
  {
    return new File(getTaskDir(taskId), "temp");
  }

  public List<File> getBaseTaskDirs()
  {
    return baseTaskDirs;
  }

  public synchronized File getBaseTaskDir(final String taskId)
  {
    if (!taskToTempDirMap.containsKey(taskId)) {
      addTask(taskId, baseTaskDirs.get(taskDirIndex));
      taskDirIndex = (taskDirIndex + 1) % baseTaskDirs.size();
    }

    return taskToTempDirMap.get(taskId);
  }

  public synchronized void addTask(final String taskId, final File taskDir)
  {
    final File existingTaskDir = taskToTempDirMap.get(taskId);
    if (existingTaskDir != null && !existingTaskDir.equals(taskDir)) {
      throw new ISE("Task [%s] is already assigned to worker path[%s]", taskId, existingTaskDir.getPath());
    }

    taskToTempDirMap.put(taskId, taskDir);
  }

  public synchronized void removeTask(final String taskId)
  {
    taskToTempDirMap.remove(taskId);
  }
}
