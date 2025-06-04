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

package org.apache.druid.testing.cluster.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClientImpl;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;

public class FaultyOverlordClient extends OverlordClientImpl
{
  private static final Logger log = new Logger(FaultyOverlordClient.class);

  private final ClusterTestingTaskConfig testingConfig;

  @Inject
  public FaultyOverlordClient(
      ClusterTestingTaskConfig testingConfig,
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator
  )
  {
    super(
        clientFactory.makeClient(
            NodeRole.COORDINATOR.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(6).build()
        ),
        jsonMapper
    );
    this.testingConfig = testingConfig;
  }
}
