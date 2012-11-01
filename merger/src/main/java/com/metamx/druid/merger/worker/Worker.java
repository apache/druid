/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.worker;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.worker.config.WorkerConfig;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class Worker
{
  private static final Logger log = new Logger(Worker.class);

  private final String host;

  private final ConcurrentHashMap<String, TaskStatus> runningTasks;

  public Worker(
      WorkerConfig config
  )
  {
    this(
        config.getHost()
    );
  }

  @JsonCreator
  public Worker(
      @JsonProperty("host") String host
  )
  {
    this.host = host;
    this.runningTasks = new ConcurrentHashMap<String, TaskStatus>();
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  public Map<String, TaskStatus> getTasks()
  {
    return runningTasks;
  }

  public Map<String, String> getStringProps()
  {
    return ImmutableMap.<String, String>of(
        "host", host
    );
  }

  public TaskStatus addTask(TaskStatus status)
  {
    return runningTasks.put(status.getId(), status);
  }

  public TaskStatus removeTask(String taskId)
  {
    return runningTasks.remove(taskId);
  }
}
