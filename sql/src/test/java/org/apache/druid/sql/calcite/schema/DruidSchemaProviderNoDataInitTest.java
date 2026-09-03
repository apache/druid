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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestTimelineServerView;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

public class DruidSchemaProviderNoDataInitTest extends CalciteTestBase
{
  private static final BrokerSegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = BrokerSegmentMetadataCacheConfig.create();

  @Test
  public void testInitializationWithNoData() throws Exception
  {
    final NoopEscalator escalator = new NoopEscalator();
    try (final Closer closer = Closer.create()) {
      final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
      final BrokerSegmentMetadataCache cache = new BrokerSegmentMetadataCache(
          CalciteTests.createMockQueryLifecycleFactory(
              SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate),
              conglomerate
          ),
          new TestTimelineServerView(Collections.emptyList()),
          SEGMENT_CACHE_CONFIG_DEFAULT,
          escalator,
          new InternalQueryConfig(),
          new NoopServiceEmitter(),
          new PhysicalDatasourceMetadataFactory(
              new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
              new SegmentManager(EasyMock.createMock(SegmentLocalCacheManager.class))
          ),
          null,
          CentralizedDatasourceSchemaConfig.create()
      );

      cache.start();
      cache.awaitInitialization();
      final DruidSchemaProvider druidSchemaProvider = new DruidSchemaProvider(
          CalciteTests.DRUID_SCHEMA_NAME,
          cache,
          null,
          CatalogResolver.NULL_RESOLVER,
          new PlannerConfig(),
          CalciteTests.TEST_AUTHORIZER_MAPPER
      );

      final Set<String> providedDruidTables =
          Iterables.getOnlyElement(druidSchemaProvider.getSchemas(escalator.createEscalatedAuthenticationResult()))
                   .getSchema()
                   .tables()
                   .getNames(LikePattern.any());
      Assertions.assertEquals(ImmutableSet.of(), providedDruidTables);
    }
  }
}
