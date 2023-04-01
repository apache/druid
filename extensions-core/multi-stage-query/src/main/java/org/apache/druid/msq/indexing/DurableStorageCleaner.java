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
import org.apache.druid.indexing.overlord.helpers.OverlordHelper;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.storage.StorageConnector;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * This method polls the durable storage for any stray directories, i.e. the ones that donot have a controller task
 * associated with it and cleans them periodically.
 * This ensures that the tasks which that have exited abruptly or have failed to clean up the durable storage themselves
 * donot pollute it with worker outputs and temporary files. See {@link DurableStorageCleanerConfig} for the configs.
 */
public class DurableStorageCleaner implements OverlordHelper
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
  public void schedule(ScheduledExecutorService exec)
  {
    LOG.info("Starting the DurableStorageCleaner with the config [%s]", config);

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        Duration.standardSeconds(config.getDelaySeconds()),
        // Added the initial delay explicitly so that we don't have to manually return the signal in the runnable
        Duration.standardSeconds(config.getDelaySeconds()),
        () -> {
          try {
            LOG.info("Starting the run of durable storage cleaner");
            Optional<TaskRunner> taskRunnerOptional = taskMasterProvider.get().getTaskRunner();
            if (!taskRunnerOptional.isPresent()) {
              LOG.info("Durable storage cleaner not running since the node is not the leader");
              return;
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
              LOG.info("DurableStorageCleaner did not find any left over directories to delete");
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Number of files [%d] that do not have a corresponding MSQ task associated with it. These are:\n[%s]\nT",
                    filesToRemove.size(),
                    filesToRemove
                );
              } else {
                LOG.info(
                    "Number of files [%d] that do not have a corresponding MSQ task associated with it.",
                    filesToRemove.size()
                );
              }
              storageConnector.deleteFiles(filesToRemove);
            }
          }
          catch (IOException e) {
            throw new RuntimeException("Error while running the scheduled durable storage cleanup helper", e);
          }
        }
    );

  }
}
