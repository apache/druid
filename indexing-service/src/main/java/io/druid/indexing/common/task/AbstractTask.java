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
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.query.Query;
import io.druid.query.QueryRunner;

import java.io.IOException;

public abstract class AbstractTask implements Task
{
  private static final Joiner ID_JOINER = Joiner.on("_");

  @JsonIgnore
  private final String id;

  @JsonIgnore
  private final String groupId;

  @JsonIgnore
  private final TaskResource taskResource;

  @JsonIgnore
  private final String dataSource;

  protected AbstractTask(String id, String dataSource)
  {
    this(id, id, new TaskResource(id, 1), dataSource);
  }

  protected AbstractTask(String id, String groupId, String dataSource)
  {
    this(id, groupId, new TaskResource(id, 1), dataSource);
  }

  protected AbstractTask(String id, String groupId, TaskResource taskResource, String dataSource)
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.taskResource = Preconditions.checkNotNull(taskResource, "resource");
    this.dataSource = Preconditions.checkNotNull(dataSource.toLowerCase(), "dataSource");
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

  @JsonProperty("resource")
  @Override
  public TaskResource getTaskResource()
  {
    return taskResource;
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

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    return null;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("type", getType())
                  .add("dataSource", dataSource)
                  .toString();
  }

  /**
   * Start helper methods *
   */
  public static String joinId(Object... objects)
  {
    return ID_JOINER.join(objects);
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

    if (!id.equals(that.id)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return id.hashCode();
  }

  protected Iterable<TaskLock> getTaskLocks(TaskToolbox toolbox) throws IOException
  {
    return toolbox.getTaskActionClient().submit(new LockListAction());
  }
}
