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

package io.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.coordinator.TaskRunner;

import java.io.File;

public class ExecutorLifecycleFactory
{
  private final File taskFile;
  private final File statusFile;

  public ExecutorLifecycleFactory(File taskFile, File statusFile)
  {
    this.taskFile = taskFile;
    this.statusFile = statusFile;
  }

  public ExecutorLifecycle build(TaskRunner taskRunner, ObjectMapper jsonMapper)
  {
    return new ExecutorLifecycle(
        new ExecutorLifecycleConfig().setTaskFile(taskFile).setStatusFile(statusFile), taskRunner, jsonMapper
    );
  }
}
