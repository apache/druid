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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;

import javax.annotation.Nullable;

/**
 * A holder for task-specific properties applicable to {@code CliPeon} servers.
 * For all other servers, the getters in this holder may return {@code null}.
 */
public class TaskPropertiesHolder
{
  @Nullable
  private final String dataSource;

  @Nullable
  private final String taskId;

  private final LookupLoadingSpec lookupLoadingSpec;

  private final BroadcastDatasourceLoadingSpec broadcastDatasourceLoadingSpec;

  public TaskPropertiesHolder()
  {
    this.dataSource = null;
    this.taskId = null;
    this.lookupLoadingSpec = LookupLoadingSpec.ALL;
    this.broadcastDatasourceLoadingSpec = BroadcastDatasourceLoadingSpec.ALL;
  }
  
  public TaskPropertiesHolder(
    final String dataSource,
    final String taskId,
    final LookupLoadingSpec lookupLoadingSpec,
    final BroadcastDatasourceLoadingSpec broadcastDatasourceLoadingSpec
  )
  {
    this.dataSource = dataSource;
    this.taskId = taskId;
    this.lookupLoadingSpec = lookupLoadingSpec;
    this.broadcastDatasourceLoadingSpec = broadcastDatasourceLoadingSpec;
  }

  /**
   * @return the taskId for CliPeon servers; {@code null} for all other servers.
   */
  @Nullable
  public String getDataSource()
  {
    return dataSource;
  }

  /**
   * @return the dataSource for CliPeon servers; {@code null} for all other servers.
   */
  @Nullable
  public String getTaskId()
  {
    return taskId;
  }

  /**
   * @return the loading spec for a given task.
   */
  public LookupLoadingSpec getLookupLoadingSpec()
  {
    return lookupLoadingSpec;
  }

  /**
   * @return the broadcast datasource loading spec.
   */
  public BroadcastDatasourceLoadingSpec getBroadcastDatasourceLoadingSpec()
  {
    return broadcastDatasourceLoadingSpec;
  }
}
