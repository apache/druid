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

import org.apache.druid.indexing.common.task.Task;

import java.util.List;

/**
 * Result of TaskLockbox#syncFromStorage()
 */
class SyncResult
{
  private final List<Task> tasksToFail;

  SyncResult(List<Task> tasksToFail)
  {
    this.tasksToFail = tasksToFail;
  }

  /**
   * Return list of tasks whose status needs to be set to failed due to lock re-acquisition failure
   */
  List<Task> getTasksToFail()
  {
    return tasksToFail;
  }
}
