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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalTaskActionClient implements TaskActionClient
{
  private static final EmittingLogger log = new EmittingLogger(LocalTaskActionClient.class);

  private final ConcurrentHashMap<Class<? extends TaskAction>, AtomicInteger> actionCountMap = new ConcurrentHashMap<>();

  private final Task task;
  private final TaskStorage storage;
  private final TaskActionToolbox toolbox;
  private final TaskAuditLogConfig auditLogConfig;

  public LocalTaskActionClient(
      Task task,
      TaskStorage storage,
      TaskActionToolbox toolbox,
      TaskAuditLogConfig auditLogConfig
  )
  {
    this.task = task;
    this.storage = storage;
    this.toolbox = toolbox;
    this.auditLogConfig = auditLogConfig;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    if (auditLogConfig.isEnabled() && taskAction.isAudited()) {
      // Add audit log
      try {
        final long auditLogStartTime = System.currentTimeMillis();
        storage.addAuditLog(task, taskAction);
        emitTimerMetric("task/action/log/time", System.currentTimeMillis() - auditLogStartTime);
      }
      catch (Exception e) {
        final String actionClass = taskAction.getClass().getName();
        log.makeAlert(e, "Failed to record action in audit log")
           .addData("task", task.getId())
           .addData("actionClass", actionClass)
           .emit();
        throw new ISE(e, "Failed to record action [%s] in audit log", actionClass);
      }
    }

    final long performStartTime = System.currentTimeMillis();
    final RetType result = taskAction.perform(task, toolbox);
    emitTimerMetric("task/action/run/time", System.currentTimeMillis() - performStartTime);
    actionCountMap.computeIfAbsent(taskAction.getClass(), k -> new AtomicInteger()).incrementAndGet();
    return result;
  }

  @VisibleForTesting
  public int getActionCount(Class<? extends TaskAction> actionClass)
  {
    final AtomicInteger count = actionCountMap.get(actionClass);
    return count == null ? 0 : count.get();
  }

  private void emitTimerMetric(final String metric, final long time)
  {
    final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    toolbox.getEmitter().emit(metricBuilder.build(metric, Math.max(0, time)));
  }
}
