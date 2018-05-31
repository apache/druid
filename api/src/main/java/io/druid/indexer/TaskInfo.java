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
package io.druid.indexer;

import org.joda.time.DateTime;

public class TaskInfo
{
  private String id;
  private DateTime createdTime;
  private TaskState state;
  private String dataSource;

  private TaskInfo(String id, DateTime createdTime, TaskState state, String datasource)
  {
    this.id = id;
    this.createdTime = createdTime;
    this.state = state;
    this.dataSource = datasource;
  }

  private TaskInfo()
  {

  }

  public String getId()
  {
    return id;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public TaskState getState()
  {
    return state;
  }

  public String getDataSource()
  {
    return dataSource;
  }


  public static class TaskInfoBuilder
  {
    private String id;
    private DateTime createdTime;
    private TaskState state;
    private String dataSource;

    public TaskInfo.TaskInfoBuilder withId(String id)
    {
      this.id = id;
      return this;
    }

    public TaskInfo.TaskInfoBuilder withCreatedTime(DateTime getCreatedTime)
    {
      this.createdTime = getCreatedTime;
      return this;
    }

    public TaskInfo.TaskInfoBuilder withState(TaskState state)
    {
      this.state = state;
      return this;
    }

    public TaskInfo.TaskInfoBuilder withDatasource(String datasource)
    {
      this.dataSource = datasource;
      return this;
    }

    public TaskInfo build()
    {
      TaskInfo t = new TaskInfo();
      t.id = this.id;
      t.createdTime = this.createdTime;
      t.state = this.state;
      t.dataSource = this.dataSource;
      return t;
    }

    public TaskInfo build(String id, DateTime createdTime, TaskState state, String datasource)
    {
      return new TaskInfo(id, createdTime, state, datasource);
    }
  }

}


