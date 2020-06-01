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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.ISE;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractFixedIntervalTask extends AbstractTask
{
  @JsonIgnore
  private final Interval interval;

  protected AbstractFixedIntervalTask(
      String id,
      String dataSource,
      Interval interval,
      Map<String, Object> context
  )
  {
    this(id, id, new TaskResource(id, 1), dataSource, interval, context);
  }

  protected AbstractFixedIntervalTask(
      String id,
      String groupId,
      String dataSource,
      Interval interval,
      Map<String, Object> context
  )
  {
    this(id, groupId, new TaskResource(id, 1), dataSource, interval, context);
  }

  protected AbstractFixedIntervalTask(
      String id,
      String groupId,
      TaskResource taskResource,
      String dataSource,
      Interval interval,
      Map<String, Object> context
  )
  {
    super(id, groupId, taskResource, dataSource, context);
    this.interval = Preconditions.checkNotNull(interval, "interval");
    Preconditions.checkArgument(interval.toDurationMillis() > 0, "interval empty");
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return taskActionClient.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval)) != null;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
  }

  TaskLock getAndCheckLock(TaskToolbox toolbox) throws IOException
  {
    // Confirm we have a lock (will throw if there isn't exactly one element)
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox.getTaskActionClient()));

    if (!myLock.getDataSource().equals(getDataSource())) {
      throw new ISE("Lock dataSource[%s] != task dataSource[%s]", myLock.getDataSource(), getDataSource());
    }

    if (!myLock.getInterval().equals(getInterval())) {
      throw new ISE("Lock interval[%s] != task interval[%s]", myLock.getInterval(), getInterval());
    }
    return myLock;
  }
}
