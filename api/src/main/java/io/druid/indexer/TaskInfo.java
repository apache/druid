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

/**
 * This class is used to store task info from runner query and cache in OverlordResource
 */
public class TaskInfo
{
  private final String id;
  private final DateTime createdTime;
  private final TaskState state;
  private final String dataSource;

  public TaskInfo(String id, DateTime createdTime, TaskState state, String datasource)
  {
    this.id = id;
    this.createdTime = createdTime;
    this.state = state;
    this.dataSource = datasource;
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
}


