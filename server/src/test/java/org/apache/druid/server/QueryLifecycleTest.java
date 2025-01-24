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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContextTest;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class QueryLifecycleTest
{
  private static final String DATASOURCE = "some_datasource";
  private static final String IDENTITY = "some_identity";
  private static final String AUTHORIZER = "some_authorizer";

  private static final Resource RESOURCE = new Resource(DATASOURCE, ResourceType.DATASOURCE);

  private final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                              .dataSource(DATASOURCE)
                                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                                              .aggregators(new CountAggregatorFactory("chocula"))
                                              .build();
  QueryRunnerFactoryConglomerate conglomerate;
  QuerySegmentWalker texasRanger;
  GenericQueryMetricsFactory metricsFactory;
  ServiceEmitter emitter;
  RequestLogger requestLogger;
  AuthorizerMapper authzMapper;
  DefaultQueryConfig queryConfig;

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
    conglomerate = EasyMock.createMock(QueryRunnerFactoryConglomerate.class);
    texasRanger = EasyMock.createMock(QuerySegmentWalker.class);
    metricsFactory = EasyMock.createMock(GenericQueryMetricsFactory.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    requestLogger = EasyMock.createNiceMock(RequestLogger.class);
    authorizer = EasyMock.createMock(Authorizer.class);
    authzMapper = new AuthorizerMapper(ImmutableMap.of(AUTHORIZER, authorizer));
    queryConfig = EasyMock.createMock(DefaultQueryConfig.class);

    toolChest = EasyMock.createMock(QueryToolChest.class);
    runner = EasyMock.createMock(QueryRunner.class);
    metrics = EasyMock.createNiceMock(QueryMetrics.class);
    authenticationResult = EasyMock.createMock(AuthenticationResult.class);
  }

  private QueryLifecycle createLifecycle(AuthConfig authConfig)
  {
    long nanos = System.nanoTime();
    long millis = System.currentTimeMillis();
    return new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        queryConfig,
        authConfig,
        millis,
        nanos
    );
  }

  @After
  public void teardown()
  {
    EasyMock.verify(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        queryConfig,
        toolChest,
        runner,
        metrics,
        authenticationResult,
        authorizer
    );
  }

  @Test
  public void testRunSimple_preauthorizedAsSuperuser()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner)
            .once();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).once();

    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, AuthorizationResult.ALLOW_NO_RESTRICTION);
  }

  @Test
  public void testRunSimpleUnauthorized()
  {
    expectedException.expect(DruidException.class);
    expectedException.expectMessage("Unexpected state [UNAUTHORIZED], expecting [AUTHORIZED]");

    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, AuthorizationResult.DENY);
  }

  @Test
  public void testRunSimple_withPolicyRestriction()
  {
    // Test the path when an external client send a sql query to broker, through runSimple.
    Policy rowFilterPolicy = RowFilterPolicy.from(new NullFilter("some-column", null));
    AuthorizationResult authorizationResult = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        DATASOURCE,
        Optional.of(rowFilterPolicy)
    ));
    DataSource expectedDataSource = RestrictedDataSource.create(TableDataSource.create(DATASOURCE), rowFilterPolicy);

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest).once();
    // We're expecting the data source in the query to be transformed to a RestrictedDataSource, with policy.
    // Any other DataSource would throw AssertionError.
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(
        queryMatchDataSource(expectedDataSource),
        EasyMock.anyObject()
    )).andReturn(runner).once();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).once();
    replayAll();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.runSimple(query, authenticationResult, authorizationResult);
  }

  @Test
  public void testRunSimple_withPolicyRestriction_segmentMetadataQueryRunAsInternal()
  {
    // Test the path when broker sends SegmentMetadataQuery to historical, through runSimple.
    // The druid-internal gets a NoRestrictionPolicy.
    AuthorizationResult authorizationResult = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        DATASOURCE,
        Optional.of(NoRestrictionPolicy.instance())
    ));
    final SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                             .dataSource(DATASOURCE)
                                             .intervals(ImmutableList.of(Intervals.ETERNITY))
                                             .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest).once();
    // We're expecting the data source in the query to still be TableDataSource.
    // Any other DataSource would throw AssertionError.
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(
        queryMatchDataSource(TableDataSource.create(DATASOURCE)),
        EasyMock.anyObject()
    )).andReturn(runner).once();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).once();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, authorizationResult);
  }


  @Test
  public void testRunSimple_withPolicyRestriction_segmentMetadataQueryRunAsExternal()
  {
    Policy policy = RowFilterPolicy.from(new NullFilter("col", null));
    AuthorizationResult authorizationResult = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        DATASOURCE,
        Optional.of(policy)
    ));
    final SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                             .dataSource(DATASOURCE)
                                             .intervals(ImmutableList.of(Intervals.ETERNITY))
                                             .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest).once();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).once();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    Assert.assertThrows(Exception.class, () -> lifecycle.runSimple(query, authenticationResult, authorizationResult));
  }

  @Test
  public void testRunSimple_withoutPolicy()
  {
    AuthorizationResult authorizationResult = AuthorizationResult.ALLOW_NO_RESTRICTION;
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner)
            .anyTimes();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).anyTimes();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(AuthConfig.newBuilder().build());
    lifecycle.runSimple(query, authenticationResult, authorizationResult);
  }

  @Test
  public void testRunSimple_foundMultiplePolicyRestrictions()
  {
    // Multiple policy restrictions indicates most likely the system is trying to double-authorizing the request
    // This is not allowed in any case.
    expectedException.expect(ISE.class);
    expectedException.expectMessage(
        "Multiple restrictions on table [some_datasource]: policy [RowFilterPolicy{rowFilter=some-column IS NULL}] and policy [RowFilterPolicy{rowFilter=some-column2 IS NULL}]");

    DimFilter originalFilterOnRDS = new NullFilter("some-column", null);
    Policy originalFilterPolicy = RowFilterPolicy.from(originalFilterOnRDS);

    Policy newFilterPolicy = RowFilterPolicy.from(new NullFilter("some-column2", null));
    AuthorizationResult authorizationResult = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        DATASOURCE,
        Optional.of(newFilterPolicy)
    ));

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(RestrictedDataSource.create(
                                            TableDataSource.create(DATASOURCE),
                                            originalFilterPolicy
                                        ))
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).anyTimes();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner).anyTimes();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).anyTimes();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, authorizationResult);
  }

  @Test
  public void testRunSimple_queryWithRestrictedDataSource_policyRestrictionMightHaveBeenRemoved()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(
        "No restriction found on table [some_datasource], but had policy [RowFilterPolicy{rowFilter=some-column IS NULL}] before.");

    DimFilter originalFilterOnRDS = new NullFilter("some-column", null);
    Policy originalFilterPolicy = RowFilterPolicy.from(originalFilterOnRDS);
    DataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create(DATASOURCE),
        originalFilterPolicy
    );

    // The query is built on a restricted data source, but we didn't find any policy, which could be one of:
    // 1. policy restriction might have been removed
    // 2. some bug in the system
    // In this case, we throw an exception to be safe.
    AuthorizationResult authorizationResult = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        DATASOURCE,
        Optional.empty()
    ));

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(restrictedDataSource)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).anyTimes();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner).anyTimes();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).anyTimes();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, authorizationResult);
  }

  @Test
  public void testAuthorized_withPolicyRestriction()
  {
    // Test the path broker receives a native json query from external client, should add restriction on data source
    Policy rowFilterPolicy = RowFilterPolicy.from(new NullFilter("some-column", null));
    Access access = Access.allowWithRestriction(rowFilterPolicy);

    DataSource expectedDataSource = RestrictedDataSource.create(TableDataSource.create(DATASOURCE), rowFilterPolicy);

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, RESOURCE, Action.READ))
            .andReturn(access).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest).anyTimes();
    // We're expecting the data source in the query to be transformed to a RestrictedDataSource, with policy.
    // Any other DataSource would throw AssertionError.
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(
                queryMatchDataSource(expectedDataSource),
                EasyMock.anyObject()
            ))
            .andReturn(runner).anyTimes();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).anyTimes();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).allowBasicAccess());
    lifecycle.execute();
  }

  @Test
  public void testAuthorized_queryWithRestrictedDataSource_runWithSuperUserPermission()
  {
    // Test the path historical receives a native json query from broker, query already has restriction on data source
    Policy rowFilterPolicy = RowFilterPolicy.from(new NullFilter("some-column", null));
    // Internal druid system would get a NO_RESTRICTION on a restricted data source.
    Access access = Access.allowWithRestriction(NoRestrictionPolicy.instance());

    DataSource restrictedDataSource = RestrictedDataSource.create(TableDataSource.create(DATASOURCE), rowFilterPolicy);

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(restrictedDataSource)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .build();
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, RESOURCE, Action.READ))
            .andReturn(access).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest).anyTimes();
    // We're expecting the data source in the query to be the same RestrictedDataSource.
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(
                queryMatchDataSource(restrictedDataSource),
                EasyMock.anyObject()
            ))
            .andReturn(runner).anyTimes();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).anyTimes();
    replayAll();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).allowBasicAccess());
    lifecycle.execute();
  }

  @Test
  public void testAuthorizeQueryContext_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource(DATASOURCE, ResourceType.DATASOURCE),
                Action.READ
            ))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("foo", ResourceType.QUERY_CONTEXT),
                Action.WRITE
            ))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("baz", ResourceType.QUERY_CONTEXT),
                Action.WRITE
            ))
            .andReturn(Access.OK).times(2);

    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final Map<String, Object> userContext = ImmutableMap.of("foo", "bar", "baz", "qux");
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(userContext)
                                        .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = new HashMap<>(lifecycle.getQuery().getContext());
    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    revisedContext.remove("queryId");
    Assert.assertEquals(
        userContext,
        revisedContext
    );

    Assert.assertTrue(lifecycle.authorize(mockRequest()).allowAccessWithNoRestriction());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).allowAccessWithNoRestriction());
  }

  @Test
  public void testAuthorizeQueryContext_notAuthorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource(DATASOURCE, ResourceType.DATASOURCE),
                Action.READ
            ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("foo", ResourceType.QUERY_CONTEXT),
                Action.WRITE
            ))
            .andReturn(Access.DENIED)
            .times(2);

    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(ImmutableMap.of("foo", "bar"))
                                        .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(authenticationResult).allowBasicAccess());
  }

  @Test
  public void testAuthorizeQueryContext_unsecuredKeys()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, RESOURCE, Action.READ))
            .andReturn(Access.OK)
            .times(2);

    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final Map<String, Object> userContext = ImmutableMap.of("foo", "bar", "baz", "qux");
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(userContext)
                                        .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .setUnsecuredContextKeys(ImmutableSet.of("foo", "baz"))
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = new HashMap<>(lifecycle.getQuery().getContext());
    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    revisedContext.remove("queryId");
    Assert.assertEquals(
        userContext,
        revisedContext
    );

    Assert.assertTrue(lifecycle.authorize(mockRequest()).allowAccessWithNoRestriction());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).allowAccessWithNoRestriction());
  }

  @Test
  public void testAuthorizeQueryContext_securedKeys()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource(DATASOURCE, ResourceType.DATASOURCE),
                Action.READ
            ))
            .andReturn(Access.OK)
            .times(2);

    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final Map<String, Object> userContext = ImmutableMap.of("foo", "bar", "baz", "qux");
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(userContext)
                                        .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      // We have secured keys, just not what the user gave.
                                      .setSecuredContextKeys(ImmutableSet.of("foo2", "baz2"))
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = new HashMap<>(lifecycle.getQuery().getContext());
    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    revisedContext.remove("queryId");
    Assert.assertEquals(
        userContext,
        revisedContext
    );

    Assert.assertTrue(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).allowBasicAccess());
  }

  @Test
  public void testAuthorizeQueryContext_securedKeysNotAuthorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource(DATASOURCE, ResourceType.DATASOURCE),
                Action.READ
            ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("foo", ResourceType.QUERY_CONTEXT),
                Action.WRITE
            ))
            .andReturn(Access.DENIED)
            .times(2);

    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final Map<String, Object> userContext = ImmutableMap.of("foo", "bar", "baz", "qux");
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(userContext)
                                        .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      // We have secured keys. User used one of them.
                                      .setSecuredContextKeys(ImmutableSet.of("foo", "baz2"))
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(authenticationResult).allowBasicAccess());
  }

  @Test
  public void testAuthorizeLegacyQueryContext_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("fake", ResourceType.DATASOURCE),
                Action.READ
            ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("foo", ResourceType.QUERY_CONTEXT),
                Action.WRITE
            ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(
                authenticationResult,
                new Resource("baz", ResourceType.QUERY_CONTEXT),
                Action.WRITE
            ))
            .andReturn(Access.OK)
            .times(2);

    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final QueryContextTest.LegacyContextQuery query = new QueryContextTest.LegacyContextQuery(ImmutableMap.of(
        "foo",
        "bar",
        "baz",
        "qux"
    ));

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = lifecycle.getQuery().getContext();
    Assert.assertNotNull(revisedContext);
    Assert.assertTrue(revisedContext.containsKey("foo"));
    Assert.assertTrue(revisedContext.containsKey("baz"));
    Assert.assertTrue(revisedContext.containsKey("queryId"));

    Assert.assertTrue(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(mockRequest()).allowBasicAccess());
  }

  public static Query<?> queryMatchDataSource(DataSource dataSource)
  {
    EasyMock.reportMatcher(new IArgumentMatcher()
    {
      @Override
      public boolean matches(Object query)
      {
        return query instanceof Query
               && ((Query<?>) query).getDataSource().equals(dataSource);
      }

      @Override
      public void appendTo(StringBuffer buffer)
      {
        buffer.append("dataSource(\"").append(dataSource).append("\")");
      }
    });
    return null;
  }

  private HttpServletRequest mockRequest()
  {
    HttpServletRequest request = EasyMock.createNiceMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(EasyMock.eq(AuthConfig.DRUID_AUTHENTICATION_RESULT)))
            .andReturn(authenticationResult).anyTimes();
    EasyMock.expect(request.getAttribute(EasyMock.eq(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)))
            .andReturn(null).anyTimes();
    EasyMock.expect(request.getAttribute(EasyMock.eq(AuthConfig.DRUID_AUTHORIZATION_CHECKED)))
            .andReturn(null).anyTimes();
    EasyMock.replay(request);
    return request;
  }

  private void replayAll()
  {
    EasyMock.replay(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        queryConfig,
        toolChest,
        runner,
        metrics,
        authenticationResult,
        authorizer
    );
  }
}
