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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.TaskServiceClients;
import org.joda.time.Duration;

public class ParallelIndexSupervisorTaskClientProviderImpl implements ParallelIndexSupervisorTaskClientProvider
{
  private final ServiceClientFactory serviceClientFactory;
  private final OverlordClient overlordClient;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;

  @Inject
  public ParallelIndexSupervisorTaskClientProviderImpl(
      @EscalatedGlobal ServiceClientFactory serviceClientFactory,
      OverlordClient overlordClient,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper
  )
  {
    this.serviceClientFactory = serviceClientFactory;
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
  }

  @Override
  public ParallelIndexSupervisorTaskClient build(
      final String supervisorTaskId,
      final Duration httpTimeout,
      final long numRetries
  )
  {
    return new ParallelIndexSupervisorTaskClientImpl(
        TaskServiceClients.makeClient(
            supervisorTaskId,
            StandardRetryPolicy.builder().maxAttempts(numRetries - 1).build(),
            serviceClientFactory,
            overlordClient
        ),
        jsonMapper,
        smileMapper,
        httpTimeout
    );
  }
}
