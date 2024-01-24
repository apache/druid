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
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestTimelineServerView;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class DruidSchemaNoDataInitTest extends CalciteTestBase
{
  private static final BrokerSegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = BrokerSegmentMetadataCacheConfig.create();

  @Test
  public void testInitializationWithNoData() throws Exception
  {
    try (final Closer closer = Closer.create()) {
      final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
      final BrokerSegmentMetadataCache cache = new BrokerSegmentMetadataCache(
          CalciteTests.createMockQueryLifecycleFactory(
              SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate),
              conglomerate
          ),
          new TestTimelineServerView(Collections.emptyList()),
          SEGMENT_CACHE_CONFIG_DEFAULT,
          new NoopEscalator(),
          new InternalQueryConfig(),
          new NoopServiceEmitter(),
          new PhysicalDatasourceMetadataFactory(
              new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
              new SegmentManager(EasyMock.createMock(SegmentLoader.class))),
          null
      );

      cache.start();
      cache.awaitInitialization();
      final DruidSchema druidSchema = new DruidSchema(cache, null, CatalogResolver.NULL_RESOLVER);

      Assert.assertEquals(ImmutableSet.of(), druidSchema.getTableNames());
    }
  }
}
