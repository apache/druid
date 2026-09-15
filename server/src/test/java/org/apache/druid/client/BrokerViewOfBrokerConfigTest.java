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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.DefaultQueryBlocklistRule;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.broker.QueryConfigSnapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

public class BrokerViewOfBrokerConfigTest
{
  private BrokerViewOfBrokerConfig target;

  private CoordinatorClient coordinatorClient;
  private BrokerDynamicConfig config;
  private DefaultQueryConfig defaultQueryConfig;


  @BeforeEach
  public void setUp() throws Exception
  {
    config = BrokerDynamicConfig.builder().build();
    defaultQueryConfig = new DefaultQueryConfig(ImmutableMap.of("timeout", 30000, "useCache", true));
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.getBrokerDynamicConfig()).thenReturn(Futures.immediateFuture(config));
    target = new BrokerViewOfBrokerConfig(coordinatorClient, defaultQueryConfig);
  }

  @Test
  public void testFetchesConfigOnStartup()
  {
    target.start();
    Mockito.verify(coordinatorClient, Mockito.times(1)).getBrokerDynamicConfig();
    Assertions.assertEquals(config, target.getDynamicConfig());
  }

  @Test
  public void testResolvedContextMergesDynamicOverStaticDefaults()
  {
    // Dynamic config overrides "useCache" and adds "priority"; static "timeout" is preserved.
    final BrokerDynamicConfig dynamicConfig = BrokerDynamicConfig.builder()
                                                                 .withQueryContext(
                                                                     QueryContext.of(ImmutableMap.of("useCache", false, "priority", 5))
                                                                 )
                                                                 .build();
    target.setDynamicConfig(dynamicConfig);

    final Map<String, Object> resolved = target.getContext();
    Assertions.assertEquals(30000, resolved.get("timeout"));
    Assertions.assertEquals(false, resolved.get("useCache"));
    Assertions.assertEquals(5, resolved.get("priority"));
  }

  @Test
  public void testResolvedContextEqualsStaticDefaultsWhenDynamicContextIsEmpty()
  {
    target.setDynamicConfig(BrokerDynamicConfig.builder().build());
    Assertions.assertEquals(defaultQueryConfig.getContext(), target.getContext());
  }

  @Test
  public void testSnapshotBeforeFirstSyncHasStaticDefaultsAndNoDynamicConfig()
  {
    final QueryConfigSnapshot snapshot = target.snapshotForQuery();
    Assertions.assertEquals(defaultQueryConfig.getContext(), snapshot.getResolvedDefaultQueryContext());
    Assertions.assertTrue(snapshot.getQueryBlocklist().isEmpty());
  }

  @Test
  public void testSnapshotAndGetContextStayInSync()
  {
    final BrokerDynamicConfig dynamicConfig =
        BrokerDynamicConfig.builder()
                           .withQueryContext(QueryContext.of(ImmutableMap.of("priority", 5)))
                           .withQueryBlocklist(ImmutableList.of(
                               new DefaultQueryBlocklistRule("block-ds", ImmutableSet.of("ds"), null, null)
                           ))
                           .build();
    target.setDynamicConfig(dynamicConfig);

    final QueryConfigSnapshot snapshot = target.snapshotForQuery();
    Assertions.assertSame(target.getContext(), snapshot.getResolvedDefaultQueryContext());
    Assertions.assertEquals(dynamicConfig.getQueryBlocklist(), snapshot.getQueryBlocklist());
    Assertions.assertEquals(5, snapshot.getResolvedDefaultQueryContext().get("priority"));
    Assertions.assertSame(snapshot.getDynamicConfig(), target.getDynamicConfig());
  }

  @Test
  public void testSnapshotIsUnaffectedByLaterConfigUpdate()
  {
    // A query holds its snapshot for the whole lifecycle, so a later swap must not change it.
    target.setDynamicConfig(
        BrokerDynamicConfig.builder()
                           .withQueryContext(QueryContext.of(ImmutableMap.of("priority", 5)))
                           .build()
    );
    final QueryConfigSnapshot snapshot = target.snapshotForQuery();

    target.setDynamicConfig(
        BrokerDynamicConfig.builder()
                           .withQueryContext(QueryContext.of(ImmutableMap.of("priority", 9)))
                           .build()
    );

    Assertions.assertEquals(5, snapshot.getResolvedDefaultQueryContext().get("priority"));
    Assertions.assertEquals(9, target.getContext().get("priority"));
  }
}
