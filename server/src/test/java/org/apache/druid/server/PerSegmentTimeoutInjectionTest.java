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

package org.apache.druid.server;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.broker.PerSegmentTimeoutConfig;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PerSegmentTimeoutInjectionTest
{
  private static final String DATASOURCE = "my_datasource";

  private QueryRunnerFactoryConglomerate conglomerate;
  private QuerySegmentWalker texasRanger;
  private GenericQueryMetricsFactory metricsFactory;
  private ServiceEmitter emitter;
  private RequestLogger requestLogger;
  private AuthorizerMapper authzMapper;
  private QueryConfigProvider queryConfig;
  private QueryToolChest toolChest;

  private final TimeseriesQuery baseQuery = Druids.newTimeseriesQueryBuilder()
      .dataSource(DATASOURCE)
      .intervals(List.of(Intervals.ETERNITY))
      .aggregators(new CountAggregatorFactory("count"))
      .build();

  @Before
  public void setUp()
  {
    conglomerate = EasyMock.createMock(QueryRunnerFactoryConglomerate.class);
    texasRanger = EasyMock.createMock(QuerySegmentWalker.class);
    metricsFactory = EasyMock.createMock(GenericQueryMetricsFactory.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    requestLogger = EasyMock.createNiceMock(RequestLogger.class);
    authzMapper = EasyMock.createNiceMock(AuthorizerMapper.class);
    queryConfig = EasyMock.createMock(QueryConfigProvider.class);
    toolChest = EasyMock.createNiceMock(QueryToolChest.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(conglomerate, queryConfig);
  }

  @Test
  public void testPerDatasourceTimeout_applied()
  {
    Map<String, PerSegmentTimeoutConfig> config = Map.of(
        DATASOURCE, new PerSegmentTimeoutConfig(5000, false)
    );

    expectDefaults();

    QueryLifecycle lifecycle = createLifecycle(config);
    lifecycle.initialize(baseQuery);

    Assert.assertEquals(5000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  @Test
  public void testPerDatasourceTimeout_userOverrideWins()
  {
    Map<String, PerSegmentTimeoutConfig> config = Map.of(
        DATASOURCE, new PerSegmentTimeoutConfig(5000, false)
    );

    expectDefaults();

    TimeseriesQuery queryWithUserTimeout = baseQuery.withOverriddenContext(
        Map.of(QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 2000L)
    );

    QueryLifecycle lifecycle = createLifecycle(config);
    lifecycle.initialize(queryWithUserTimeout);

    Assert.assertEquals(2000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  @Test
  public void testPerDatasourceTimeout_monitorOnlyDoesNotInject()
  {
    // monitorOnly=true: config exists but should NOT be enforced
    Map<String, PerSegmentTimeoutConfig> config = Map.of(
        DATASOURCE, new PerSegmentTimeoutConfig(5000, true)
    );

    expectDefaults();

    QueryLifecycle lifecycle = createLifecycle(config);
    lifecycle.initialize(baseQuery);

    Assert.assertFalse(
        "monitorOnly should not inject perSegmentTimeout",
        lifecycle.getQuery().context().usePerSegmentTimeout()
    );
  }

  @Test
  public void testPerDatasourceTimeout_noMatchingDatasource()
  {
    Map<String, PerSegmentTimeoutConfig> config = Map.of(
        "other_datasource", new PerSegmentTimeoutConfig(5000, false)
    );

    expectDefaults();

    QueryLifecycle lifecycle = createLifecycle(config);
    lifecycle.initialize(baseQuery);

    Assert.assertFalse(lifecycle.getQuery().context().usePerSegmentTimeout());
  }

  @Test
  public void testPerDatasourceTimeout_overridesSystemDefault()
  {
    Map<String, PerSegmentTimeoutConfig> config = Map.of(
        DATASOURCE, new PerSegmentTimeoutConfig(5000, false)
    );

    // System default sets perSegmentTimeout to 10000
    EasyMock.expect(queryConfig.getContext())
            .andReturn(Map.of(QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 10000L))
            .anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    EasyMock.replay(conglomerate, queryConfig);

    QueryLifecycle lifecycle = createLifecycle(config);
    lifecycle.initialize(baseQuery);

    Assert.assertEquals(5000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  @Test
  public void testPrecedence_userOverridesPerDatasourceOverridesSystemDefault()
  {
    // System default: 10000, per-datasource: 5000, user: 2000 — user should win
    Map<String, PerSegmentTimeoutConfig> config = Map.of(
        DATASOURCE, new PerSegmentTimeoutConfig(5000, false)
    );

    EasyMock.expect(queryConfig.getContext())
            .andReturn(Map.of(QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 10000L))
            .anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    EasyMock.replay(conglomerate, queryConfig);

    TimeseriesQuery queryWithUserTimeout = baseQuery.withOverriddenContext(
        Map.of(QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 2000L)
    );

    QueryLifecycle lifecycle = createLifecycle(config);
    lifecycle.initialize(queryWithUserTimeout);

    Assert.assertEquals(2000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  private void expectDefaults()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(Map.of()).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    EasyMock.replay(conglomerate, queryConfig);
  }

  private QueryLifecycle createLifecycle(Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig)
  {
    return new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        queryConfig,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        Collections.emptyList(),
        perSegmentTimeoutConfig,
        System.currentTimeMillis(),
        System.nanoTime()
    );
  }
}
