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
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
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
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QueryLifecycleTest
{
  private static final String DATASOURCE = "some_datasource";
  private static final String IDENTITY = "some_identity";
  private static final String AUTHORIZER = "some_authorizer";

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
  AuthConfig authConfig;

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
    authorizer = EasyMock.createMock(Authorizer.class);
    authzMapper = new AuthorizerMapper(ImmutableMap.of(AUTHORIZER, authorizer));
    queryConfig = EasyMock.createMock(DefaultQueryConfig.class);
    authConfig = EasyMock.createMock(AuthConfig.class);

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
        authConfig,
        millis,
        nanos
    );

    toolChest = EasyMock.createMock(QueryToolChest.class);
    runner = EasyMock.createMock(QueryRunner.class);
    metrics = EasyMock.createNiceMock(QueryMetrics.class);
    authenticationResult = EasyMock.createMock(AuthenticationResult.class);
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

  @Test
  public void testAuthorizeQueryContext_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authConfig.authorizeQueryContextParams()).andReturn(true).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("baz", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();

    replayAll();

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(ImmutableMap.of("foo", "bar", "baz", "qux"))
                                        .build();

    lifecycle.initialize(query);

    Assert.assertEquals(
        ImmutableMap.of("foo", "bar", "baz", "qux"),
        lifecycle.getQuery().getQueryContext().getUserParams()
    );
    Assert.assertTrue(lifecycle.getQuery().getQueryContext().getMergedParams().containsKey("queryId"));
    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));

    Assert.assertTrue(lifecycle.authorize(mockRequest()).isAllowed());
  }

  @Test
  public void testAuthorizeQueryContext_notAuthorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authConfig.authorizeQueryContextParams()).andReturn(true).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource(DATASOURCE, ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(new Access(false));

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();

    replayAll();

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .intervals(ImmutableList.of(Intervals.ETERNITY))
                                        .aggregators(new CountAggregatorFactory("chocula"))
                                        .context(ImmutableMap.of("foo", "bar"))
                                        .build();

    lifecycle.initialize(query);
    Assert.assertFalse(lifecycle.authorize(mockRequest()).isAllowed());
  }

  @Test
  public void testAuthorizeLegacyQueryContext_authorized()
  {
    EasyMock.expect(queryConfig.getContext()).andReturn(ImmutableMap.of()).anyTimes();
    EasyMock.expect(authConfig.authorizeQueryContextParams()).andReturn(true).anyTimes();
    EasyMock.expect(authenticationResult.getIdentity()).andReturn(IDENTITY).anyTimes();
    EasyMock.expect(authenticationResult.getAuthorizerName()).andReturn(AUTHORIZER).anyTimes();
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("fake", ResourceType.DATASOURCE), Action.READ))
            .andReturn(Access.OK);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("foo", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK);
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("baz", ResourceType.QUERY_CONTEXT), Action.WRITE)).andReturn(Access.OK);
    // to use legacy query context with context authorization, even system generated things like queryId need to be explicitly added
    EasyMock.expect(authorizer.authorize(authenticationResult, new Resource("queryId", ResourceType.QUERY_CONTEXT), Action.WRITE))
            .andReturn(Access.OK);

    EasyMock.expect(toolChestWarehouse.getToolChest(EasyMock.anyObject()))
            .andReturn(toolChest)
            .once();

    replayAll();

    final LegacyContextQuery query = new LegacyContextQuery(ImmutableMap.of("foo", "bar", "baz", "qux"));

    lifecycle.initialize(query);

    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("foo"));
    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("baz"));
    Assert.assertTrue(lifecycle.getQuery().getContext().containsKey("queryId"));

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
        authConfig,
        toolChest,
        runner,
        metrics,
        authenticationResult,
        authorizer
    );
  }

  private static class LegacyContextQuery implements Query
  {
    private final Map<String, Object> context;

    private LegacyContextQuery(Map<String, Object> context)
    {
      this.context = context;
    }

    @Override
    public DataSource getDataSource()
    {
      return new TableDataSource("fake");
    }

    @Override
    public boolean hasFilters()
    {
      return false;
    }

    @Override
    public DimFilter getFilter()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "legacy-context-query";
    }

    @Override
    public QueryRunner getRunner(QuerySegmentWalker walker)
    {
      return new NoopQueryRunner();
    }

    @Override
    public List<Interval> getIntervals()
    {
      return Collections.singletonList(Intervals.ETERNITY);
    }

    @Override
    public Duration getDuration()
    {
      return getIntervals().get(0).toDuration();
    }

    @Override
    public Granularity getGranularity()
    {
      return Granularities.ALL;
    }

    @Override
    public DateTimeZone getTimezone()
    {
      return DateTimeZone.UTC;
    }

    @Override
    public Map<String, Object> getContext()
    {
      return context;
    }

    @Override
    public boolean getContextBoolean(String key, boolean defaultValue)
    {
      if (context == null || !context.containsKey(key)) {
        return defaultValue;
      }
      return (boolean) context.get(key);
    }

    @Override
    public boolean isDescending()
    {
      return false;
    }

    @Override
    public Ordering getResultOrdering()
    {
      return Ordering.natural();
    }

    @Override
    public Query withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      return new LegacyContextQuery(context);
    }

    @Override
    public Query withId(String id)
    {
      context.put(BaseQuery.QUERY_ID, id);
      return this;
    }

    @Nullable
    @Override
    public String getId()
    {
      return (String) context.get(BaseQuery.QUERY_ID);
    }

    @Override
    public Query withSubQueryId(String subQueryId)
    {
      context.put(BaseQuery.SUB_QUERY_ID, subQueryId);
      return this;
    }

    @Nullable
    @Override
    public String getSubQueryId()
    {
      return (String) context.get(BaseQuery.SUB_QUERY_ID);
    }

    @Override
    public Query withDataSource(DataSource dataSource)
    {
      return this;
    }

    @Override
    public Query withOverriddenContext(Map contextOverride)
    {
      return new LegacyContextQuery(contextOverride);
    }

    @Override
    public Object getContextValue(String key, Object defaultValue)
    {
      if (!context.containsKey(key)) {
        return defaultValue;
      }
      return context.get(key);
    }

    @Override
    public Object getContextValue(String key)
    {
      return context.get(key);
    }
  }
}
