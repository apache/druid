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

package org.apache.druid.client;

import com.google.common.util.concurrent.Futures;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

public class BrokerViewOfCoordinatorConfigTest
{
  private BrokerViewOfCoordinatorConfig target;

  private CoordinatorClient coordinatorClient;
  private CoordinatorDynamicConfig config;


  @Before
  public void setUp() throws Exception
  {
    config = CoordinatorDynamicConfig.builder()
                                     .withCloneServers(Map.of("host1", "host2"))
                                     .build();
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.getCoordinatorDynamicConfig()).thenReturn(Futures.immediateFuture(config));
    target = new BrokerViewOfCoordinatorConfig(coordinatorClient);
  }

  @Test
  public void testFetchesConfigOnStartup()
  {
    target.start();
    Mockito.verify(coordinatorClient, Mockito.times(1)).getCoordinatorDynamicConfig();
    Assert.assertEquals(config, target.getDynamicConfig());
  }

  @Test
  public void testGetQueryableServersForCloneQueryModes()
  {
    target.start();

    final QueryableDruidServer cloneTarget = queryableHistorical("host1");
    final QueryableDruidServer cloneSource = queryableHistorical("host2");
    final QueryableDruidServer unrelated = queryableHistorical("host3");
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers = new Int2ObjectRBTreeMap<>();
    historicalServers.put(0, Set.of(cloneTarget, cloneSource, unrelated));

    Assert.assertEquals(
        Set.of(cloneTarget),
        target.getQueryableServers(historicalServers, CloneQueryMode.EXCLUDESOURCE).get(0)
    );
    Assert.assertEquals(
        Set.of(cloneTarget, unrelated),
        target.getQueryableServers(historicalServers, CloneQueryMode.PREFERCLONES).get(0)
    );
    Assert.assertEquals(
        Set.of(cloneSource, unrelated),
        target.getQueryableServers(historicalServers, CloneQueryMode.EXCLUDECLONES).get(0)
    );
    Assert.assertSame(
        historicalServers,
        target.getQueryableServers(historicalServers, CloneQueryMode.INCLUDECLONES)
    );
  }

  private static QueryableDruidServer queryableHistorical(final String host)
  {
    return new QueryableDruidServer(
        new DruidServer(host, host, null, 1, null, ServerType.HISTORICAL, "__default", 0),
        null
    );
  }
}
