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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.broker.PerSegmentTimeoutConfig;
import org.apache.druid.server.broker.QueryConfigSnapshot;
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
import java.util.Set;

/**
 * Precedence of the per-query dynamic overrides at the {@link QueryLifecycle} level: an override beats a
 * non-client-provided value in the context, but a value the client set wins. The datasource-matching logic itself is
 * tested in {@code org.apache.druid.server.broker.BrokerDynamicConfigTest}.
 */
public class PerSegmentTimeoutInjectionTest
{
  private static final String DATASOURCE = "my_datasource";
  private static final String KEY = QueryContexts.PER_SEGMENT_TIMEOUT_KEY;

  private QueryRunnerFactoryConglomerate conglomerate;
  private QuerySegmentWalker texasRanger;
  private GenericQueryMetricsFactory metricsFactory;
  private ServiceEmitter emitter;
  private RequestLogger requestLogger;
  private AuthorizerMapper authzMapper;
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
    texasRanger = EasyMock.createNiceMock(QuerySegmentWalker.class);
    metricsFactory = EasyMock.createNiceMock(GenericQueryMetricsFactory.class);
    emitter = EasyMock.createNiceMock(ServiceEmitter.class);
    requestLogger = EasyMock.createNiceMock(RequestLogger.class);
    authzMapper = EasyMock.createNiceMock(AuthorizerMapper.class);
    toolChest = EasyMock.createNiceMock(QueryToolChest.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(conglomerate);
  }

  @Test
  public void testDynamicOverrideAppliedWhenClientDidNotSet()
  {
    QueryLifecycle lifecycle = createLifecycle(perSegmentTimeout(5000));
    lifecycle.initialize(baseQuery);

    Assert.assertEquals(5000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  @Test
  public void testDynamicOverridesNonClientValueInContext()
  {
    // SQL path: a default was merged into the context but the client did not set it, so the dynamic override wins.
    TimeseriesQuery query = baseQuery.withOverriddenContext(Map.of(KEY, "0"));

    QueryLifecycle lifecycle = createLifecycle(perSegmentTimeout(5000));
    lifecycle.initialize(query, Collections.emptySet());

    Assert.assertEquals(5000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  @Test
  public void testClientProvidedValueWins()
  {
    TimeseriesQuery query = baseQuery.withOverriddenContext(Map.of(KEY, 2000L));

    QueryLifecycle lifecycle = createLifecycle(perSegmentTimeout(5000));
    lifecycle.initialize(query, Set.of(KEY));

    Assert.assertEquals(2000L, lifecycle.getQuery().context().getPerSegmentTimeout());
  }

  @Test
  public void testNoDynamicConfigMeansNoInjection()
  {
    QueryLifecycle lifecycle = createLifecycle(null);
    lifecycle.initialize(baseQuery);

    Assert.assertFalse(lifecycle.getQuery().context().usePerSegmentTimeout());
  }

  private static BrokerDynamicConfig perSegmentTimeout(long timeoutMs)
  {
    return BrokerDynamicConfig.builder()
                              .withPerSegmentTimeoutConfig(Map.of(DATASOURCE, new PerSegmentTimeoutConfig(timeoutMs, false)))
                              .build();
  }

  private QueryLifecycle createLifecycle(BrokerDynamicConfig dynamicConfig)
  {
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    EasyMock.replay(conglomerate);

    return new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        new QueryConfigSnapshot(Collections.emptyMap(), dynamicConfig),
        System.currentTimeMillis(),
        System.nanoTime()
    );
  }
}
