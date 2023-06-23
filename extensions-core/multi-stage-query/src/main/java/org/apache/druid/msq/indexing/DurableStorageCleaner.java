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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.duty.DutySchedule;
import org.apache.druid.indexing.overlord.duty.OverlordDuty;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.storage.StorageConnector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This duty polls the durable storage for any stray directories, i.e. the ones that donot have a controller task
 * associated with it and cleans them periodically.
 * This ensures that the tasks which that have exited abruptly or have failed to clean up the durable storage themselves
 * donot pollute it with worker outputs and temporary files. See {@link DurableStorageCleanerConfig} for the configs.
 */
public class DurableStorageCleaner implements OverlordDuty
{
  private static final Logger LOG = new Logger(DurableStorageCleaner.class);

  private final DurableStorageCleanerConfig config;
  private final StorageConnector storageConnector;
  private final Provider<TaskMaster> taskMasterProvider;

  @Inject
  public DurableStorageCleaner(
      final DurableStorageCleanerConfig config,
      final @MultiStageQuery StorageConnector storageConnector,
      @JacksonInject final Provider<TaskMaster> taskMasterProvider
  )
  {
    this.config = config;
    this.storageConnector = storageConnector;
    this.taskMasterProvider = taskMasterProvider;
  }

  @Override
  public boolean isEnabled()
  {
    return config.isEnabled();
  }

  @Override
  public void run() throws Exception
  {
    Optional<TaskRunner> taskRunnerOptional = taskMasterProvider.get().getTaskRunner();
    if (!taskRunnerOptional.isPresent()) {
      LOG.info("DurableStorageCleaner not running since the node is not the leader");
      return;
    } else {
      LOG.info("Running DurableStorageCleaner");
    }

    TaskRunner taskRunner = taskRunnerOptional.get();
    Iterator<String> allFiles = storageConnector.listDir("");
    Set<String> runningTaskIds = taskRunner.getRunningTasks()
                                           .stream()
                                           .map(TaskRunnerWorkItem::getTaskId)
                                           .map(DurableStorageUtils::getControllerDirectory)
                                           .collect(Collectors.toSet());

    Set<String> filesToRemove = new HashSet<>();
    while (allFiles.hasNext()) {
      String currentFile = allFiles.next();
      String taskIdFromPathOrEmpty = DurableStorageUtils.getControllerTaskIdWithPrefixFromPath(currentFile);
      if (taskIdFromPathOrEmpty != null && !taskIdFromPathOrEmpty.isEmpty()) {
        if (runningTaskIds.contains(taskIdFromPathOrEmpty)) {
          // do nothing
        } else {
          filesToRemove.add(currentFile);
        }
      }
    }

    if (filesToRemove.isEmpty()) {
      LOG.info("There are no leftover directories to delete.");
    } else {
      LOG.info(
          "Removing [%d] files which are not associated with any MSQ task.",
          filesToRemove.size()
      );
      LOG.debug("Files to remove:\n[%s]\n", filesToRemove);
      storageConnector.deleteFiles(filesToRemove);
    }
  }

  @Override
  public DutySchedule getSchedule()
  {
    final long delayMillis = config.getDelaySeconds() * 1000;
    return new DutySchedule(delayMillis, delayMillis);
  }
}
