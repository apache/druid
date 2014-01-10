/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.actions;

import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskStorage;

import java.io.IOException;

public class LocalTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final TaskStorage storage;
  private final TaskActionToolbox toolbox;

  private static final EmittingLogger log = new EmittingLogger(LocalTaskActionClient.class);

  public LocalTaskActionClient(Task task, TaskStorage storage, TaskActionToolbox toolbox)
  {
    this.task = task;
    this.storage = storage;
    this.toolbox = toolbox;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    if (taskAction.isAudited()) {
      // Add audit log
      try {
        storage.addAuditLog(task, taskAction);
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

    return taskAction.perform(task, toolbox);
  }
}
