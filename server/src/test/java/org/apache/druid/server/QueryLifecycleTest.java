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
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import org.apache.druid.client.BrokerViewOfBrokerConfig;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryContextTest;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.broker.QueryConfigSnapshot;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@LazySingleton
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
  QueryToolChest toolChest;
  @Bind
  QueryRunnerFactoryConglomerate conglomerate;

  QueryRunner runner;
  @Bind
  QuerySegmentWalker texasRanger;
  @Bind
  GenericQueryMetricsFactory metricsFactory;
  @Bind
  ServiceEmitter emitter;
  @Bind
  RequestLogger requestLogger;

  Authorizer authorizer;
  @Bind
  AuthorizerMapper authzMapper;

  QueryConfigProvider queryConfig;
  @Bind(lazy = true)
  QueryConfigProvider queryConfigProvider;

  @Bind(lazy = true)
  AuthConfig authConfig;
  @Bind(lazy = true)
  PolicyEnforcer policyEnforcer;
  @Bind(lazy = true)
  @Nullable
  BrokerViewOfBrokerConfig brokerViewOfBrokerConfig;

  QueryMetrics metrics;
  AuthenticationResult authenticationResult;

  @Inject
  QueryLifecycleFactory queryLifecycleFactory;

  Injector injector;

  @BeforeEach
  public void setup()
  {
    conglomerate = EasyMock.createMock(QueryRunnerFactoryConglomerate.class);
    texasRanger = EasyMock.createMock(QuerySegmentWalker.class);
    metricsFactory = EasyMock.createMock(GenericQueryMetricsFactory.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    requestLogger = EasyMock.createNiceMock(RequestLogger.class);
    authorizer = EasyMock.createMock(Authorizer.class);
    authzMapper = new AuthorizerMapper(ImmutableMap.of(AUTHORIZER, authorizer));
    queryConfig = EasyMock.createMock(QueryConfigProvider.class);
    queryConfigProvider = queryConfig;

    toolChest = EasyMock.createMock(QueryToolChest.class);
    runner = EasyMock.createMock(QueryRunner.class);
    metrics = EasyMock.createNiceMock(QueryMetrics.class);
    authenticationResult = EasyMock.createMock(AuthenticationResult.class);
    authConfig = new AuthConfig();
    policyEnforcer = NoopPolicyEnforcer.instance();

    injector = Guice.createInjector(
        BoundFieldModule.of(this),
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          DruidBinders.dataSourceQueryHandlerBinder(binder);
        }
    );
  }

  private QueryLifecycle createLifecycle()
  {
    injector.injectMembers(this);
    return queryLifecycleFactory.factorize();
  }

  @AfterEach
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
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner)
            .once();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).once();

    replayAll();

    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.runSimple(query, authenticationResult, AuthorizationResult.ALLOW_NO_RESTRICTION);
  }

  /**
   * Native query form of:
   *
   * <pre>{@code
   * SELECT COUNT(*)
   * FROM
   * (
   *   SELECT
   *     task_id,
   *     COUNT(*) AS task_count
   *   FROM sys.tasks
   *   GROUP BY task_id
   * )
   * WHERE task_count > 0
   * }</pre>
   */
  @Test
  public void testSystemTableHandlerHandlesQueryDataSource()
  {
    final Query<?> innerQuery = Druids.newScanQueryBuilder()
                                      .dataSource(new SystemTableDataSource("tasks"))
                                      .eternityInterval()
                                      .build();
    assertWrappedSystemTableUsesHandler(new QueryDataSource(innerQuery));
  }

  /**
   * Native query form of:
   *
   * <pre>{@code
   * SELECT COUNT(*)
   * FROM sys.tasks
   * WHERE type = 'index_parallel'
   * }</pre>
   */
  @Test
  public void testSystemTableHandlerHandlesFilteredDataSource()
  {
    assertWrappedSystemTableUsesHandler(
        FilteredDataSource.create(
            new SystemTableDataSource("tasks"),
            new SelectorDimFilter("type", "index_parallel", null)
        )
    );
  }

  @Test
  public void testRunSimpleUnauthorized()
  {
    Throwable exception = Assertions.assertThrows(DruidException.class, () -> {

      EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
      EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
      EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
          .andReturn(toolChest)
          .once();
      EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();
      replayAll();

      QueryLifecycle lifecycle = createLifecycle();
      lifecycle.runSimple(query, authenticationResult, AuthorizationResult.DENY);
    });
    Assertions.assertTrue(exception.getMessage().contains("Unexpected state [UNAUTHORIZED], expecting [AUTHORIZED]"));
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

    authConfig = AuthConfig.newBuilder()
                           .setAuthorizeQueryContextParams(true)
                           .build();
    QueryLifecycle lifecycle = createLifecycle();
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
    EasyMock.expect(texasRanger.getQueryRunnerForIntervals(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(runner)
            .once();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();
    EasyMock.expect(runner.run(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Sequences.empty()).once();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle();
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
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).once();
    replayAll();

    QueryLifecycle lifecycle = createLifecycle();
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> lifecycle.runSimple(query, authenticationResult, authorizationResult)
    );
    Assertions.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertEquals(
        "You do not have permission to run a segmentMetadata query on table[some_datasource].",
        e.getMessage()
    );
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

    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.runSimple(query, authenticationResult, authorizationResult);
  }

  @Test
  public void testRunSimple_foundDifferentPolicyRestrictions()
  {
    Throwable exception = Assertions.assertThrows(ISE.class, () -> {

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

      QueryLifecycle lifecycle = createLifecycle();
      lifecycle.runSimple(query, authenticationResult, authorizationResult);
    });
    Assertions.assertTrue(exception.getMessage().contains("Different restrictions on table [some_datasource]: previous policy [RowFilterPolicy{rowFilter=some-column IS NULL}] and new policy [RowFilterPolicy{rowFilter=some-column2 IS NULL}]"));
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

    policyEnforcer = new RestrictAllTablesPolicyEnforcer(ImmutableList.of(
        NoRestrictionPolicy.class.getName(),
        RowFilterPolicy.class.getName()
    ));
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertTrue(lifecycle.authorize(authenticationResult).allowBasicAccess());
    // Success, query has a RowFilterPolicy, and is allowed by PolicyEnforcer.
    lifecycle.execute();
  }

  @Test
  public void testAuthorized_withPolicyRestriction_failedSecurityValidation()
  {
    // Test the path broker receives a native json query from external client, should add restriction on data source
    Policy rowFilterPolicy = RowFilterPolicy.from(new NullFilter("some-column", null));
    Access access = Access.allowWithRestriction(rowFilterPolicy);

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
    replayAll();

    policyEnforcer = new RestrictAllTablesPolicyEnforcer(ImmutableList.of(NoRestrictionPolicy.class.getName()));
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);
    // Fail, only NoRestrictionPolicy is allowed.
    DruidException e = Assertions.assertThrows(DruidException.class, () -> lifecycle.authorize(authenticationResult));
    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assertions.assertEquals("Failed security validation with dataSource [some_datasource]", e.getMessage());
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

    authConfig = AuthConfig.newBuilder().setAuthorizeQueryContextParams(true).build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertTrue(lifecycle.authorize(authenticationResult).allowBasicAccess());
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

    authConfig = AuthConfig.newBuilder().setAuthorizeQueryContextParams(true).build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = new HashMap<>(lifecycle.getQuery().getContext());
    Assertions.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    revisedContext.remove("queryId");
    Assertions.assertEquals(
        userContext,
        revisedContext
    );

    Assertions.assertTrue(lifecycle.authorize(mockRequest()).allowAccessWithNoRestriction());

    lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertTrue(lifecycle.authorize(authenticationResult).allowAccessWithNoRestriction());
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

    authConfig = AuthConfig.newBuilder().setAuthorizeQueryContextParams(true).build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertFalse(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertFalse(lifecycle.authorize(authenticationResult).allowBasicAccess());
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

    authConfig = AuthConfig.newBuilder()
                           .setAuthorizeQueryContextParams(true)
                           .setUnsecuredContextKeys(ImmutableSet.of("foo", "baz"))
                           .build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = new HashMap<>(lifecycle.getQuery().getContext());
    Assertions.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    revisedContext.remove("queryId");
    Assertions.assertEquals(
        userContext,
        revisedContext
    );

    Assertions.assertTrue(lifecycle.authorize(mockRequest()).allowAccessWithNoRestriction());

    lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertTrue(lifecycle.authorize(authenticationResult).allowAccessWithNoRestriction());
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

    authConfig = AuthConfig.newBuilder()
                           .setAuthorizeQueryContextParams(true)
                           // We have secured keys, just not what the user gave.
                           .setSecuredContextKeys(ImmutableSet.of("foo2", "baz2"))
                           .build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = new HashMap<>(lifecycle.getQuery().getContext());
    Assertions.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    revisedContext.remove("queryId");
    Assertions.assertEquals(
        userContext,
        revisedContext
    );

    Assertions.assertTrue(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertTrue(lifecycle.authorize(authenticationResult).allowBasicAccess());
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

    authConfig = AuthConfig.newBuilder()
                           .setAuthorizeQueryContextParams(true)
                           // We have secured keys. User used one of them.
                           .setSecuredContextKeys(ImmutableSet.of("foo", "baz2"))
                           .build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertFalse(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertFalse(lifecycle.authorize(authenticationResult).allowBasicAccess());
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

    authConfig = AuthConfig.newBuilder()
                           .setAuthorizeQueryContextParams(true)
                           .build();
    QueryLifecycle lifecycle = createLifecycle();
    lifecycle.initialize(query);

    final Map<String, Object> revisedContext = lifecycle.getQuery().getContext();
    Assertions.assertNotNull(revisedContext);
    Assertions.assertTrue(revisedContext.containsKey("foo"));
    Assertions.assertTrue(revisedContext.containsKey("baz"));
    Assertions.assertTrue(revisedContext.containsKey("queryId"));

    Assertions.assertTrue(lifecycle.authorize(mockRequest()).allowBasicAccess());

    lifecycle = createLifecycle();
    lifecycle.initialize(query);
    Assertions.assertTrue(lifecycle.authorize(mockRequest()).allowBasicAccess());
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

  @Test
  public void testRunSimple_queryBlocklisted()
  {
    // Create a blocklist rule that matches our test query
    QueryBlocklistRule rule = new DefaultQueryBlocklistRule(
        "test-rule",
        ImmutableSet.of(DATASOURCE),
        null,
        null
    );

    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();
    // When exception is thrown, metrics are still emitted
    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).once();

    replayAll();

    // Create lifecycle with blocklist
    QueryLifecycle lifecycle = new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        authConfig,
        policyEnforcer,
        new QueryConfigSnapshot(
            ImmutableMap.of(),
            BrokerDynamicConfig.builder().withQueryBlocklist(ImmutableList.of(rule)).build()
        ),
        System.currentTimeMillis(),
        System.nanoTime()
    );

    // This should throw because query matches blocklist rule
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> lifecycle.runSimple(query, authenticationResult, AuthorizationResult.ALLOW_NO_RESTRICTION)
    );
    Assertions.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
    Assertions.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assertions.assertTrue(e.getMessage().contains("blocked by rule"));
    Assertions.assertTrue(e.getMessage().contains("test-rule"));
  }

  @Test
  public void testRunSimple_queryNotBlocklisted()
  {
    // Create a blocklist rule that does NOT match our test query
    QueryBlocklistRule rule = new DefaultQueryBlocklistRule(
        "test-rule",
        ImmutableSet.of("other_datasource"),
        null,
        null
    );

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

    // Create lifecycle with blocklist
    QueryLifecycle lifecycle = new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        authConfig,
        policyEnforcer,
        new QueryConfigSnapshot(
            ImmutableMap.of(),
            BrokerDynamicConfig.builder().withQueryBlocklist(ImmutableList.of(rule)).build()
        ),
        System.currentTimeMillis(),
        System.nanoTime()
    );

    // This should succeed because query doesn't match blocklist rule
    lifecycle.runSimple(query, authenticationResult, AuthorizationResult.ALLOW_NO_RESTRICTION);
  }

  private HttpServletRequest mockRequest()
  {
    return mockRequest(null);
  }

  private HttpServletRequest mockRequest(@Nullable final String nativeQueryRoute)
  {
    HttpServletRequest request = EasyMock.createNiceMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(EasyMock.eq(AuthConfig.DRUID_AUTHENTICATION_RESULT)))
            .andReturn(authenticationResult).anyTimes();
    EasyMock.expect(request.getAttribute(EasyMock.eq(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)))
            .andReturn(null).anyTimes();
    EasyMock.expect(request.getAttribute(EasyMock.eq(AuthConfig.DRUID_AUTHORIZATION_CHECKED)))
            .andReturn(null).anyTimes();
    EasyMock.expect(request.getHeader(QueryResource.HEADER_NATIVE_QUERY_ROUTE))
            .andReturn(nativeQueryRoute).anyTimes();
    EasyMock.replay(request);
    return request;
  }

  /** {@code X-Druid-Native-Query-Route: local} reaches the datasource query handler as server request state. */
  @Test
  public void testLocalNativeQueryRouteIsPassedToDataSourceHandler()
  {
    assertNativeQueryRoute(QueryResource.NATIVE_QUERY_ROUTE_LOCAL, true);
  }

  /** The native-query route header is case-sensitive and rejects non-exact values. */
  @Test
  public void testNonExactNativeQueryRouteIsNotLocal()
  {
    assertNativeQueryRoute("LOCAL", false);
  }

  private void assertNativeQueryRoute(final String route, final boolean expectedExecuteLocally)
  {
    final ScanQuery systemTableQuery = Druids.newScanQueryBuilder()
                                             .dataSource(new SystemTableDataSource("tasks"))
                                             .eternityInterval()
                                             .build();
    final AtomicInteger handlerCalls = new AtomicInteger();
    final DataSourceQueryHandler handler = new DataSourceQueryHandler()
    {
      @Override
      public <T> QueryRunner<T> createRunner(
          final Query<T> query,
          final AuthenticationResult authenticationResult,
          final boolean executeLocally
      )
      {
        handlerCalls.incrementAndGet();
        Assertions.assertEquals(expectedExecuteLocally, executeLocally);
        return (queryPlus, responseContext) -> Sequences.empty();
      }
    };

    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(
        authorizer.authorize(
            authenticationResult,
            new Resource("sys.tasks", ResourceType.DATASOURCE),
            Action.READ
        )
    ).andReturn(Access.OK).once();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    replayAll();

    final QueryLifecycle lifecycle = new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        authConfig,
        policyEnforcer,
        new QueryConfigSnapshot(ImmutableMap.of(), BrokerDynamicConfig.builder().build()),
        Map.of(SystemTableDataSource.class, handler),
        System.currentTimeMillis(),
        System.nanoTime()
    );
    lifecycle.initialize(systemTableQuery);
    Assertions.assertTrue(
        lifecycle.authorize(mockRequest(route)).allowAccessWithNoRestriction()
    );
    lifecycle.execute();

    Assertions.assertEquals(1, handlerCalls.get());
  }

  private void assertWrappedSystemTableUsesHandler(final DataSource dataSource)
  {
    final TimeseriesQuery wrappedQuery = Druids.newTimeseriesQueryBuilder()
                                               .dataSource(dataSource)
                                               .intervals(ImmutableList.of(Intervals.ETERNITY))
                                               .aggregators(new CountAggregatorFactory("count"))
                                               .build();
    final AtomicInteger handlerCalls = new AtomicInteger();
    final DataSourceQueryHandler handler = new DataSourceQueryHandler()
    {
      @Override
      public <T> QueryRunner<T> createRunner(
          final Query<T> query,
          final AuthenticationResult authenticationResult,
          final boolean executeLocally
      )
      {
        Assertions.assertFalse(executeLocally);
        handlerCalls.incrementAndGet();
        return (queryPlus, responseContext) -> Sequences.empty();
      }
    };

    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(conglomerate.getToolChest(EasyMock.anyObject())).andReturn(toolChest).once();
    replayAll();

    final QueryLifecycle lifecycle = new QueryLifecycle(
        conglomerate,
        texasRanger,
        metricsFactory,
        emitter,
        requestLogger,
        authzMapper,
        authConfig,
        policyEnforcer,
        new QueryConfigSnapshot(ImmutableMap.of(), BrokerDynamicConfig.builder().build()),
        Map.of(SystemTableDataSource.class, handler),
        System.currentTimeMillis(),
        System.nanoTime()
    );

    lifecycle.runSimple(wrappedQuery, authenticationResult, AuthorizationResult.ALLOW_NO_RESTRICTION);
    Assertions.assertEquals(1, handlerCalls.get());
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
