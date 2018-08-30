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

package org.apache.druid.indexing.overlord;

import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;

import java.util.concurrent.Executor;

/**
 * Listener to be registered with {@link TaskRunner#registerListener(TaskRunnerListener, Executor)}.
 */
public interface TaskRunnerListener
{
  String getListenerId();

  /**
   * Called when the location of a task has changed. The task may not actually be done starting up when
   * this notification arrives, so it may not be listening at this location yet.
   */
  void locationChanged(String taskId, TaskLocation newLocation);

  /**
   * Called when the status of a task has changed.
   */
  void statusChanged(String taskId, TaskStatus status);
}
