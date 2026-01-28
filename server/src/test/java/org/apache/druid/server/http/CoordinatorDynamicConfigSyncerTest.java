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

package org.apache.druid.server.http;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CoordinatorDynamicConfigSyncerTest
{
  private CoordinatorDynamicConfigSyncer target;

  private ServiceClient serviceClient;
  private CoordinatorConfigManager coordinatorConfigManager;
  private DruidNodeDiscovery druidNodeDiscovery;

  @Before
  public void setUp() throws Exception
  {
    serviceClient = mock(ServiceClient.class);

    BytesFullResponseHolder holder = mock(BytesFullResponseHolder.class);
    doReturn(HttpResponseStatus.OK).when(holder).getStatus();
    doReturn(Futures.immediateFuture(holder))
        .when(serviceClient).asyncRequest(ArgumentMatchers.any(), ArgumentMatchers.any());

    coordinatorConfigManager = mock(CoordinatorConfigManager.class);
    DruidNodeDiscoveryProvider provider = mock(DruidNodeDiscoveryProvider.class);
    druidNodeDiscovery = mock(DruidNodeDiscovery.class);
    doReturn(druidNodeDiscovery).when(provider).getForNodeRole(NodeRole.BROKER);

    target = new CoordinatorDynamicConfigSyncer(
        (serviceName, serviceLocator, retryPolicy) -> serviceClient,
        coordinatorConfigManager,
        DefaultObjectMapper.INSTANCE,
        provider,
        mock(ServiceEmitter.class)
    );
  }

  @Test
  public void testSync()
  {
    CoordinatorDynamicConfig config = CoordinatorDynamicConfig
        .builder()
        .withMaxSegmentsToMove(105)
        .withReplicantLifetime(500)
        .withReplicationThrottleLimit(5)
        .build();

    doReturn(config).when(coordinatorConfigManager).getCurrentDynamicConfig();
    List<DiscoveryDruidNode> nodes = List.of(
        new DiscoveryDruidNode(
            new DruidNode("service", "host", false, 8080, null, true, false),
            NodeRole.BROKER,
            null,
            null
        )
    );
    doReturn(nodes).when(druidNodeDiscovery).getAllNodes();

    target.broadcastConfigToBrokers();
    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/druid-internal/v1/config/coordinator")
        .jsonContent(DefaultObjectMapper.INSTANCE, config);
    verify(serviceClient).asyncRequest(eq(requestBuilder), ArgumentMatchers.any());
  }
}
