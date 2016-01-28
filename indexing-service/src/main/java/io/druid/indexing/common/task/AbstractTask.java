/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

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

  private final Map<String, Object> context;

  protected AbstractTask(String id, String dataSource, Map<String, Object> context)
  {
    this(id, id, new TaskResource(id, 1), dataSource, context);
  }

  protected AbstractTask(String id, String groupId, String dataSource, Map<String, Object> context)
  {
    this(id, groupId, new TaskResource(id, 1), dataSource, context);
  }

  protected AbstractTask(String id, String groupId, TaskResource taskResource, String dataSource, Map<String, Object> context)
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.taskResource = Preconditions.checkNotNull(taskResource, "resource");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.context = context;
  }

  public static String makeId(String id, final String typeName, String dataSource, Interval interval)
  {
    return id != null ? id : joinId(
        typeName,
        dataSource,
        interval.getStart(),
        interval.getEnd(),
        new DateTime().toString()
    );
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
  public String getClasspathPrefix()
  {
    return null;
  }

  @Override
  public boolean canRestore()
  {
    return false;
  }

  @Override
  public void stopGracefully()
  {
    // Should not be called when canRestore = false.
    throw new UnsupportedOperationException("Cannot stop gracefully");
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
   * Start helper methods
   *
   * @param objects objects to join
   *
   * @return string of joined objects
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

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public Object getContextValue(String key)
  {
    return context == null ? null : context.get(key);
  }

}
