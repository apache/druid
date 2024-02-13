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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.QueryContextTest;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinTestHelper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class QueryLifecycleTest
{
  private static final String DATASOURCE = "some_datasource";
  private static final String IDENTITY = "some_identity";
  private static final String AUTHORIZER = "some_authorizer";
  private static final String LOOKUP_COUNTRY_CODE_TO_NAME = "country_code_to_name";
  private static final String LOOKUP_COUNTRY_NUMBER_TO_NAME = "country_number_to_name";


  private final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                              .dataSource(DATASOURCE)
                                              .intervals(ImmutableList.of(Intervals.ETERNITY))
                                              .aggregators(new CountAggregatorFactory("chocula"))
                                              .build();
  private ExprMacroTable exprMacroTable;
  private VirtualColumns lookupVirtualColumns;

  static {
    NullHandling.initializeForTests();
  }

  QueryToolChestWarehouse toolChestWarehouse;
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
  public void setup() throws IOException
  {
    toolChestWarehouse = EasyMock.createMock(QueryToolChestWarehouse.class);
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

    final Map<String, String> countryCodeToNameMap = JoinTestHelper.createCountryIsoCodeToNameLookup().getMap();
    final Map<String, String> countryNumberToNameMap = JoinTestHelper.createCountryNumberToNameLookup().getMap();

    exprMacroTable = new ExprMacroTable(ImmutableList.of(new LookupExprMacro(new LookupExtractorFactoryContainerProvider()
    {
      @Override
      public Set<String> getAllLookupNames()
      {
        return ImmutableSet.of(LOOKUP_COUNTRY_CODE_TO_NAME, LOOKUP_COUNTRY_NUMBER_TO_NAME);
      }

      @Override
      public Optional<LookupExtractorFactoryContainer> get(String lookupName)
      {
        if (LOOKUP_COUNTRY_CODE_TO_NAME.equals(lookupName)) {
          return Optional.of(new LookupExtractorFactoryContainer("0",
                                                                 new MapLookupExtractorFactory(
                                                                         countryCodeToNameMap,
                                                                         false
                                                                 )
          ));
        } else if (LOOKUP_COUNTRY_NUMBER_TO_NAME.equals(lookupName)) {
          return Optional.of(new LookupExtractorFactoryContainer("0",
                                                                 new MapLookupExtractorFactory(
                                                                         countryNumberToNameMap,
                                                                         false
                                                                 )
          ));
        } else {
          return Optional.empty();
        }
      }
    })));
    lookupVirtualColumns = VirtualColumns.create(ImmutableList.of(new ExpressionVirtualColumn(
            LOOKUP_COUNTRY_CODE_TO_NAME,
            "lookup(countryIsoCode, '" + LOOKUP_COUNTRY_CODE_TO_NAME + "')",
            ColumnType.STRING,
            exprMacroTable
    )));
  }

  private QueryLifecycle createLifecycle(AuthConfig authConfig)
  {
    long nanos = System.nanoTime();
    long millis = System.currentTimeMillis();
    return new QueryLifecycle(
            toolChestWarehouse,
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
            toolChestWarehouse,
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

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, Access.OK);
  }

  @Test
  public void testRunSimpleUnauthorized()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(Access.DEFAULT_ERROR_MESSAGE);

    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();

    EasyMock.expect(toolChest.makeMetrics(EasyMock.anyObject())).andReturn(metrics).anyTimes();


    replayAll();

    QueryLifecycle lifecycle = createLifecycle(new AuthConfig());
    lifecycle.runSimple(query, authenticationResult, new Access(false));
  }

  @Test
  public void testScanQuery_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(LOOKUP_COUNTRY_CODE_TO_NAME, ResourceType.LOOKUP), Action.READ))
            .andReturn(Access.OK).times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();
    final ScanQuery scanQuery = Druids.newScanQueryBuilder()
                                      .dataSource(DATASOURCE)
                                      .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
                                      .virtualColumns(lookupVirtualColumns)
                                      .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .setEnableAuthorizeLookupDirectly(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(scanQuery);

    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(scanQuery);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testTopNQuery_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(LOOKUP_COUNTRY_CODE_TO_NAME, ResourceType.LOOKUP), Action.READ))
            .andReturn(Access.OK).times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();
    final TopNQuery topNquery = new TopNQueryBuilder().dataSource(DATASOURCE)
                                                      .granularity(Granularities.ALL)
                                                      .intervals(ImmutableList.of(Intervals.ETERNITY))
                                                      .dimension(LOOKUP_COUNTRY_CODE_TO_NAME)
                                                      .virtualColumns(lookupVirtualColumns)
                                                      .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                                      .threshold(5)
                                                      .build();


    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .setEnableAuthorizeLookupDirectly(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(topNquery);

    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(topNquery);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testGroupByQuery_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(LOOKUP_COUNTRY_CODE_TO_NAME, ResourceType.LOOKUP), Action.READ))
            .andReturn(Access.OK).times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();
    final GroupByQuery groupByQuery = GroupByQuery.builder()
                                                  .setDataSource(DATASOURCE)
                                                  .setInterval(ImmutableList.of(Intervals.ETERNITY))
                                                  .setVirtualColumns(lookupVirtualColumns)
                                                  .setGranularity(Granularities.ALL)
                                                  .build();

    AuthConfig authConfig = AuthConfig.newBuilder()
                                      .setAuthorizeQueryContextParams(true)
                                      .setEnableAuthorizeLookupDirectly(true)
                                      .build();
    QueryLifecycle lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(groupByQuery);

    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));
    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(groupByQuery);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testAuthorizeQueryContext_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK).times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("baz", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK).times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
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

    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testAuthorizeQueryContext_notAuthorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.DENIED)
            .times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
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
    Assert.assertFalse(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testAuthorizeQueryContext_unsecuredKeys()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK)
            .times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
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

    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testAuthorizeQueryContext_securedKeys()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK)
            .times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
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

    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testAuthorizeQueryContext_securedKeysNotAuthorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.DENIED)
            .times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
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
    Assert.assertFalse(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(authenticationResult).isAllowed());
  }

  @Test
  public void testAuthorizeLegacyQueryContext_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("fake", ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK)
            .times(2);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("baz", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK)
            .times(2);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .times(2);

    replayAll();

    final QueryContextTest.LegacyContextQuery query = new QueryContextTest.LegacyContextQuery(ImmutableMap.of("foo", "bar", "baz", "qux"));

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

    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());

    lifecycle = createLifecycle(authConfig);
    lifecycle.initialize(query);
    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());
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
            toolChestWarehouse,
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
