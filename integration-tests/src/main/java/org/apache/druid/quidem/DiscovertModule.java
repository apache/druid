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
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.BrokerSegmentMetadataCache;
import org.apache.druid.sql.calcite.util.CalciteTests;

import java.util.Properties;

public class DiscovertModule extends AbstractModule {

    DiscovertModule() {
    }

    @Override
    protected void configure()
    {
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
    DruidNodeDiscoveryProvider getProvider()
    {
      final DruidNode coordinatorNode = CalciteTests.mockCoordinatorNode();
      return CalciteTests.mockDruidNodeDiscoveryProvider(coordinatorNode);
    }
  }