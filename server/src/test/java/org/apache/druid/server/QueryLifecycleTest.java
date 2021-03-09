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

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class QueryLifecycleTest
{
  private static final String DATASOURCE = "some_datasource";
  private static final String IDENTITY = "some_identity";

  private final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                              .dataSource(DATASOURCE)
                                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                                              .aggregators(new CountAggregatorFactory("chocula"))
                                              .build();
  QueryToolChestWarehouse toolChestWarehouse;
  QuerySegmentWalker texasRanger;
  GenericQueryMetricsFactory metricsFactory;
  ServiceEmitter emitter;
  RequestLogger requestLogger;
  AuthorizerMapper authzMapper;
  DefaultQueryConfig queryConfig;

  QueryLifecycle lifecycle;

  QueryToolChest toolChest;
  QueryRunner runner;
  QueryMetrics metrics;
  AuthenticationResult authenticationResult;
  Authorizer authorizer;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    toolChestWarehouse = EasyMock.createMock(QueryToolChestWarehouse.class);
    texasRanger = EasyMock.createMock(QuerySegmentWalker.class);
    metricsFactory = EasyMock.createMock(GenericQueryMetricsFactory.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    requestLogger = EasyMock.createNiceMock(RequestLogger.class);
    authzMapper = EasyMock.createMock(AuthorizerMapper.class);
    queryConfig = EasyMock.createMock(DefaultQueryConfig.class);

    long nanos = System.nanoTime();
    long millis = System.currentTimeMillis();
    lifecycle = new QueryLifecycle(
        toolChestWarehouse,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        queryConfig,
        millis,
        nanos
    );

    toolChest = EasyMock.createMock(QueryToolChest.class);
    runner = EasyMock.createMock(QueryRunner.class);
    metrics = EasyMock.createNiceMock(QueryMetrics.class);
    authenticationResult = EasyMock.createMock(AuthenticationResult.class);
    authorizer = EasyMock.createMock(Authorizer.class);
  }

  @After
  public void teardown()
  {
    EasyMock.verify(
        toolChestWarehouse,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        queryConfig,
        toolChest,
        runner,
        metrics,
        authenticationResult,
        authorizer
    );
  }

  @Test
  public void testRunSimplePreauthorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner)
            .once();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).once();


    replayAll();

    lifecycle.runSimple(query, authenticationResult, Access.OK);
  }

  @Test
  public void testRunSimpleUnauthorized()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Unauthorized");

    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();

    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();


    replayAll();

    lifecycle.runSimple(query, authenticationResult, new Access(false));
  }

  private void replayAll()
  {
    EasyMock.replay(
        toolChestWarehouse,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        queryConfig,
        toolChest,
        runner,
        metrics,
        authenticationResult,
        authorizer
    );
  }
}
