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

package org.apache.druid.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.BrokerSegmentMetadataCache;
import org.apache.druid.sql.calcite.util.CalciteTests;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;

public class DiscovertModule extends AbstractModule {

    DiscovertModule() {
    }

    @Override
    protected void configure()
    {
//      builder.addModule(propOverrideModuel());
    }

    @Provides
    @LazySingleton
    public BrokerSegmentMetadataCache provideCache() {
      return null;
    }

    @Provides
    @LazySingleton
    public Properties getProps() {
      Properties localProps = new Properties();
      localProps.put("druid.enableTlsPort", "false");
      localProps.put("druid.zk.service.enabled", "false");
      localProps.put("druid.plaintextPort", "12345");
      localProps.put("druid.host", "localhost");
      localProps.put("druid.broker.segment.awaitInitializationOnStart","false");
      return localProps;
    }

    @Provides
    @LazySingleton
    public SqlEngine createMockSqlEngine(
        final QuerySegmentWalker walker,
        final QueryRunnerFactoryConglomerate conglomerate,
        @Json ObjectMapper jsonMapper    )
    {
      return new NativeSqlEngine(CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate), jsonMapper);
    }


    @Provides
    @LazySingleton
    DruidNodeDiscoveryProvider getProvider() {
      final DruidNode coordinatorNode = new DruidNode("test-coordinator", "dummy", false, 8081, null, true, false);
      DiscovertModule.FakeDruidNodeDiscoveryProvider provider = new FakeDruidNodeDiscoveryProvider(
          ImmutableMap.of(
              NodeRole.COORDINATOR, new FakeDruidNodeDiscovery(ImmutableMap.of(NodeRole.COORDINATOR, coordinatorNode))
          )
      );
      return provider;
    }

    /**
     * A fake {@link DruidNodeDiscoveryProvider} for {@link #createMockSystemSchema}.
     */
    private static class FakeDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
    {
      private final Map<NodeRole, DiscovertModule.FakeDruidNodeDiscovery> nodeDiscoveries;

      public FakeDruidNodeDiscoveryProvider(Map<NodeRole, DiscovertModule.FakeDruidNodeDiscovery> nodeDiscoveries)
      {
        this.nodeDiscoveries = nodeDiscoveries;
      }

      @Override
      public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
      {
        boolean get = nodeDiscoveries.getOrDefault(nodeRole, new FakeDruidNodeDiscovery())
                                     .getAllNodes()
                                     .stream()
                                     .anyMatch(x -> x.getDruidNode().equals(node));
        return () -> get;
      }

      @Override
      public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
      {
        return nodeDiscoveries.getOrDefault(nodeRole, new FakeDruidNodeDiscovery());
      }
    }

    private static class FakeDruidNodeDiscovery implements DruidNodeDiscovery
    {
      private final Set<DiscoveryDruidNode> nodes;

      FakeDruidNodeDiscovery()
      {
        this.nodes = new HashSet<>();
      }

      FakeDruidNodeDiscovery(Map<NodeRole, DruidNode> nodes)
      {
        this.nodes = Sets.newHashSetWithExpectedSize(nodes.size());
        nodes.forEach((k, v) -> {
          addNode(v, k);
        });
      }

      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return nodes;
      }

      void addNode(DruidNode node, NodeRole role)
      {
        final DiscoveryDruidNode discoveryNode = new DiscoveryDruidNode(node, role, ImmutableMap.of());
        this.nodes.add(discoveryNode);
      }

      @Override
      public void registerListener(Listener listener)
      {

      }
    }



  }