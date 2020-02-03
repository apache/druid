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

import org.apache.druid.indexing.common.actions.LocalTaskActionClient;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.overlord.TaskStorage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingLocalTaskActionClientForTest implements TaskActionClient
{
  private final ConcurrentHashMap<Class<? extends TaskAction>, AtomicInteger> actionCountMap =
      new ConcurrentHashMap<>();
  private final LocalTaskActionClient delegate;

  public CountingLocalTaskActionClientForTest(
      Task task,
      TaskStorage storage,
      TaskActionToolbox toolbox
  )
  {
    delegate = new LocalTaskActionClient(task, storage, toolbox, new TaskAuditLogConfig(false));
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    final RetType result = delegate.submit(taskAction);
    final TaskAction actionKey;
    if (taskAction instanceof SurrogateAction) {
      actionKey = ((SurrogateAction) taskAction).getTaskAction();
    } else {
      actionKey = taskAction;
    }
    actionCountMap.computeIfAbsent(actionKey.getClass(), k -> new AtomicInteger()).incrementAndGet();
    return result;
  }

  public int getActionCount(Class<? extends TaskAction> actionClass)
  {
    final AtomicInteger count = actionCountMap.get(actionClass);

    return count == null ? 0 : count.get();
  }
}
