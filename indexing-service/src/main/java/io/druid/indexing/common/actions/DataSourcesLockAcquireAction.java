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

package io.druid.indexing.common.actions;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.DataSourceAndInterval;

import java.util.List;

public class DataSourcesLockAcquireAction implements TaskAction<List<TaskLock>>
{
  @JsonIgnore
  private final List<DataSourceAndInterval> dataSourcesAndIntervals;

  @JsonCreator
  public DataSourcesLockAcquireAction(
      @JsonProperty("dataSourcesAndIntervals") List<DataSourceAndInterval> dataSourcesAndIntervals
  )
  {
    Preconditions.checkArgument(
        dataSourcesAndIntervals != null && dataSourcesAndIntervals.size() > 0,
        "dataSourcesAndIntervals"
    );
    this.dataSourcesAndIntervals = dataSourcesAndIntervals;
  }

  @JsonProperty
  public List<DataSourceAndInterval> getDataSourcesAndIntervals()
  {
    return dataSourcesAndIntervals;
  }

  public TypeReference<List<TaskLock>> getReturnTypeReference()
  {
    return new TypeReference<List<TaskLock>>()
    {
    };
  }

  @Override
  public List<TaskLock> perform(Task task, TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getTaskLockbox().lock(task, dataSourcesAndIntervals);
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isAudited()
  {
    return false;
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

    DataSourcesLockAcquireAction that = (DataSourcesLockAcquireAction) o;

    return dataSourcesAndIntervals.equals(that.dataSourcesAndIntervals);

  }

  @Override
  public int hashCode()
  {
    return dataSourcesAndIntervals.hashCode();
  }

  @Override
  public String toString()
  {
    return "DataSourcesLockAcquireAction{" +
           "dataSourcesAndIntervals=" + dataSourcesAndIntervals +
           '}';
  }
}
