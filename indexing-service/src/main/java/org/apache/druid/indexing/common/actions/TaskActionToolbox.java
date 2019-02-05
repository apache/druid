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

package org.apache.druid.indexing.common.actions;

import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

public class TaskActionToolbox
{
  private final TaskLockbox taskLockbox;
  private final TaskStorage taskStorage;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ServiceEmitter emitter;
  private final SupervisorManager supervisorManager;

  @Inject
  public TaskActionToolbox(
      TaskLockbox taskLockbox,
      TaskStorage taskStorage,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      ServiceEmitter emitter,
      SupervisorManager supervisorManager
  )
  {
    this.taskLockbox = taskLockbox;
    this.taskStorage = taskStorage;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.emitter = emitter;
    this.supervisorManager = supervisorManager;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public IndexerMetadataStorageCoordinator getIndexerMetadataStorageCoordinator()
  {
    return indexerMetadataStorageCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public SupervisorManager getSupervisorManager()
  {
    return supervisorManager;
  }
}
