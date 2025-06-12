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

package org.apache.druid.testing.cluster.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;

/**
 * Implementation of {@link TaskLockbox} that supports the following:
 * <ul>
 * <li>Skip cleanup of pending segments</li>
 * </ul>
 */
public class FaultyTaskLockbox extends GlobalTaskLockbox
{
  private static final Logger log = new Logger(FaultyTaskLockbox.class);

  private final ObjectMapper mapper;

  @Inject
  public FaultyTaskLockbox(
      TaskStorage taskStorage,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      ObjectMapper mapper
  )
  {
    super(taskStorage, metadataStorageCoordinator);
    this.mapper = mapper;
    log.info("Initializing FaultyTaskLockbox.");
  }

  @Override
  protected void cleanupPendingSegments(Task task)
  {
    final ClusterTestingTaskConfig testingConfig = getTestingConfig(task);
    if (testingConfig.getMetadataConfig().isCleanupPendingSegments()) {
      super.cleanupPendingSegments(task);
    } else {
      log.info(
          "Skipping cleanup of pending segments for task[%s] since it has testing config[%s].",
          task.getId(), testingConfig
      );
    }
  }

  private ClusterTestingTaskConfig getTestingConfig(Task task)
  {
    try {
      return ClusterTestingTaskConfig.forTask(task, mapper);
    }
    catch (Exception e) {
      // Just log the exception and proceed with the actual cleanup
      log.error(
          e,
          "Error while reading testing config for task[%s] with context[%s]."
          + " Using default config with no faults.",
          task.getId(), task.getContext()
      );
      return ClusterTestingTaskConfig.DEFAULT;
    }
  }
}
