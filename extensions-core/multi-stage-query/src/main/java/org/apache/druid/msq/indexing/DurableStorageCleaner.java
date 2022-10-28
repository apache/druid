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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.helpers.OverlordHelper;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.shuffle.DurableStorageOutputChannelFactory;
import org.apache.druid.storage.StorageConnector;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.HashSet;
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
  private final TaskRunner taskRunner;

  @Inject
  public DurableStorageCleaner(
      final DurableStorageCleanerConfig config,
      final StorageConnector storageConnector,
      final TaskRunner taskRunner
  )
  {
    this.config = config;
    this.storageConnector = storageConnector;
    this.taskRunner = taskRunner;
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
        Duration.standardSeconds(config.getInitialDelaySeconds()),
        Duration.standardSeconds(config.getDelaySeconds()),
        () -> {
          try {
            Set<String> allDirectories = new HashSet<>(storageConnector.ls(""));
            Set<String> runningTaskIds = taskRunner.getRunningTasks()
                                                   .stream()
                                                   .map(TaskRunnerWorkItem::getTaskId)
                                                   .map(DurableStorageOutputChannelFactory::getControllerDirectory)
                                                   .collect(Collectors.toSet());
            Set<String> unknownDirectories = Sets.difference(allDirectories, runningTaskIds);
            LOG.info(
                "Following directories donot have a corresponding MSQ task associated with it:\n%s\nThese will get cleaned up.",
                unknownDirectories
            );
            for (String unknownDirectory : unknownDirectories) {
              LOG.info("");
              storageConnector.deleteRecursively(unknownDirectory);
            }
          }
          catch (IOException e) {
            throw new RuntimeException("Error while running the scheduled durable storage cleanup helper", e);
          }
        }
    );

  }
}
