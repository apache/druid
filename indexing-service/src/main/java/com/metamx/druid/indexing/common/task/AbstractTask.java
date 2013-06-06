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

package com.metamx.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.metamx.druid.Query;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.actions.SegmentListUsedAction;
import com.metamx.druid.indexing.common.actions.TaskActionClient;
import com.metamx.druid.query.QueryRunner;
import org.joda.time.Interval;

public abstract class AbstractTask implements Task
{
  private static final Joiner ID_JOINER = Joiner.on("_");

  @JsonIgnore
  private final String id;

  @JsonIgnore
  private final String groupId;

  @JsonIgnore
  private final String availabilityGroup;

  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final Optional<Interval> interval;

  protected AbstractTask(String id, String dataSource, Interval interval)
  {
    this(id, id, id, dataSource, interval);
  }

  protected AbstractTask(String id, String groupId, String dataSource, Interval interval)
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.availabilityGroup = id;
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Optional.fromNullable(interval);
  }

  protected AbstractTask(String id, String groupId, String availabilityGroup, String dataSource, Interval interval)
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.availabilityGroup = Preconditions.checkNotNull(availabilityGroup, "availabilityGroup");
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
  public String getAvailabilityGroup()
  {
    return availabilityGroup;
  }

  @Override
  public String getNodeType()
  {
    return null;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("interval")
  @Override
  public Optional<Interval> getImplicitLockInterval()
  {
    return interval;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    return null;
  }

  @Override
  public TaskStatus preflight(TaskActionClient taskActionClient) throws Exception
  {
    return TaskStatus.running(id);
  }

  @Override
  public void shutdown()
  {
    // Do nothing.
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("type", getType())
                  .add("dataSource", dataSource)
                  .add("interval", getImplicitLockInterval())
                  .toString();
  }

  /** Start helper methods **/
  public static String joinId(Object... objects)
  {
    return ID_JOINER.join(objects);
  }

  public SegmentListUsedAction defaultListUsedAction()
  {
    return new SegmentListUsedAction(getDataSource(), getImplicitLockInterval().get());
  }

  public TaskStatus success()
  {
    return TaskStatus.success(getId());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractTask that = (AbstractTask) o;

    if (dataSource != null ? !dataSource.equals(that.dataSource) : that.dataSource != null) {
      return false;
    }
    if (groupId != null ? !groupId.equals(that.groupId) : that.groupId != null) {
      return false;
    }
    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    if (interval != null ? !interval.equals(that.interval) : that.interval != null) {
      return false;
    }

    return true;
  }
}
