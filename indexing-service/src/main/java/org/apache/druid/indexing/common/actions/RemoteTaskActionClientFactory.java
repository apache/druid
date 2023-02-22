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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;

/**
 */
public class RemoteTaskActionClientFactory implements TaskActionClientFactory
{
  private final ServiceClient overlordClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public RemoteTaskActionClientFactory(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @IndexingService final ServiceLocator serviceLocator,
      RetryPolicyConfig retryPolicyConfig
  )
  {
    this.overlordClient = clientFactory.makeClient(
        NodeRole.OVERLORD.toString(),
        serviceLocator,
        StandardRetryPolicy.builder()
            .maxAttempts(retryPolicyConfig.getMaxRetryCount() - 1)
            .minWaitMillis(retryPolicyConfig.getMinWait().toStandardDuration().getMillis())
            .maxWaitMillis(retryPolicyConfig.getMaxWait().toStandardDuration().getMillis())
            .build()
    );
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TaskActionClient create(Task task)
  {
    return new RemoteTaskActionClient(task, overlordClient, jsonMapper);
  }
}
