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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import org.joda.time.Interval;

public abstract class AbstractFixedIntervalTask extends AbstractTask
{
  @JsonIgnore
  private final Interval interval;

  protected AbstractFixedIntervalTask(
      String id,
      String dataSource,
      Interval interval
  )
  {
    this(id, id, new TaskResource(id, 1), dataSource, interval);
  }

  protected AbstractFixedIntervalTask(
      String id,
      String groupId,
      String dataSource,
      Interval interval
  )
  {
    this(id, groupId, new TaskResource(id, 1), dataSource, interval);
  }

  protected AbstractFixedIntervalTask(
      String id,
      String groupId,
      TaskResource taskResource,
      String dataSource,
      Interval interval
  )
  {
    super(id, groupId, taskResource, dataSource);
    this.interval = Preconditions.checkNotNull(interval, "interval");
    Preconditions.checkArgument(interval.toDurationMillis() > 0, "interval empty");
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return taskActionClient.submit(new LockTryAcquireAction(interval)).isPresent();
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }
}
