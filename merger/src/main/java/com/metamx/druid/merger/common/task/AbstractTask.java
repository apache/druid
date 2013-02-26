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

package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import org.joda.time.Interval;

public abstract class AbstractTask implements Task
{
  private final String id;
  private final String groupId;
  private final String dataSource;
  private final Optional<Interval> interval;

  protected AbstractTask(String id, String dataSource, Interval interval)
  {
    this(id, id, dataSource, interval);
  }

  protected AbstractTask(String id, String groupId, String dataSource, Interval interval)
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Optional.fromNullable(interval);
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("interval")
  @Override
  public Optional<Interval> getFixedInterval()
  {
    return interval;
  }

  @Override
  public TaskStatus preflight(TaskToolbox toolbox) throws Exception
  {
    return TaskStatus.running(id);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("type", getType())
                  .add("dataSource", dataSource)
                  .add("interval", getFixedInterval())
                  .toString();
  }
}
