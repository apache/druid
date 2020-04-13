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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Some task types, e.g., {@link CompactionTask} or {@link ParallelIndexSupervisorTask} create sub tasks and execute
 * them one at a time. This class holds the sub task that is currently running whatever its type is.
 *
 * This holder is supposed to be registered to {@link TaskResourceCleaner} so that {@link #accept} method is called
 * properly when {@link Task#stopGracefully} is called.
 */
public class CurrentSubTaskHolder implements Consumer<TaskConfig>
{
  /**
   * A flag to indicate the {@link Task} of this holder is already stopped and its child tasks shouldn't be
   * created. See {@link #currentSubTaskReference} for more details.
   */
  private static final Object SPECIAL_VALUE_STOPPED = new Object();

  /**
   * Reference to the sub-task that is currently running.
   *
   * When {@link #accept} is called, this class gets the reference to the current running task,
   * and calls {@link #cleanFunction} for that task. This reference will be updated to {@link #SPECIAL_VALUE_STOPPED}.
   */
  private final AtomicReference<Object> currentSubTaskReference = new AtomicReference<>();

  private final BiConsumer<Object, TaskConfig> cleanFunction;

  public CurrentSubTaskHolder(BiConsumer<Object, TaskConfig> subTaskCleaner)
  {
    this.cleanFunction = subTaskCleaner;
  }

  public boolean setTask(Object subTask)
  {
    final Object prevSpec = currentSubTaskReference.get();

    //noinspection ObjectEquality
    return prevSpec != SPECIAL_VALUE_STOPPED && currentSubTaskReference.compareAndSet(prevSpec, subTask);
  }

  public <T> T getTask()
  {
    //noinspection unchecked
    return (T) currentSubTaskReference.get();
  }

  @Override
  public void accept(TaskConfig taskConfig)
  {
    Object currentSubTask = currentSubTaskReference.getAndSet(SPECIAL_VALUE_STOPPED);
    if (currentSubTask != null) {
      cleanFunction.accept(currentSubTask, taskConfig);
    }
  }
}
