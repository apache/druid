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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;

import java.util.List;

/**
 * Task attempt history for complete {@link SubTaskSpec}s. A {@link SubTaskSpec} is complete if its last status is
 * succeeded or failed.
 */
class TaskHistory<T extends Task>
{
  private final SubTaskSpec<T> spec;
  private final List<TaskStatusPlus> attemptHistory; // old attempts to recent ones

  TaskHistory(SubTaskSpec<T> spec, List<TaskStatusPlus> attemptHistory)
  {
    attemptHistory.forEach(status -> {
      Preconditions.checkState(
          status.getStatusCode() == TaskState.SUCCESS || status.getStatusCode() == TaskState.FAILED,
          "Complete tasks should be recorded, but the state of task[%s] is [%s]",
          status.getId(),
          status.getStatusCode()
      );
    });
    this.spec = spec;
    this.attemptHistory = ImmutableList.copyOf(attemptHistory);
  }

  SubTaskSpec<T> getSpec()
  {
    return spec;
  }

  List<TaskStatusPlus> getAttemptHistory()
  {
    return attemptHistory;
  }

  boolean isEmpty()
  {
    return attemptHistory.isEmpty();
  }
}
