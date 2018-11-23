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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.task.Task;

/**
 */
public class RemoteTaskActionClientFactory implements TaskActionClientFactory
{
  private final DruidLeaderClient druidLeaderClient;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ObjectMapper jsonMapper;

  @Inject
  public RemoteTaskActionClientFactory(
      @IndexingService DruidLeaderClient leaderHttpClient,
      RetryPolicyFactory retryPolicyFactory,
      ObjectMapper jsonMapper
  )
  {
    this.druidLeaderClient = leaderHttpClient;
    this.retryPolicyFactory = retryPolicyFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TaskActionClient create(Task task)
  {
    return new RemoteTaskActionClient(task, druidLeaderClient, retryPolicyFactory, jsonMapper);
  }
}
