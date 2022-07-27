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

package org.apache.druid.sql;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlParameter;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class SqlLifecycleTest
{
  private PlannerFactory plannerFactory;
  private ServiceEmitter serviceEmitter;
  private RequestLogger requestLogger;
  private SqlLifecycleFactory sqlLifecycleFactory;
  private DefaultQueryConfig defaultQueryConfig;

  @Before
  public void setup()
  {
    this.plannerFactory = EasyMock.createMock(PlannerFactory.class);
    this.serviceEmitter = EasyMock.createMock(ServiceEmitter.class);
    this.requestLogger = EasyMock.createMock(RequestLogger.class);
    this.defaultQueryConfig = new DefaultQueryConfig(ImmutableMap.of("DEFAULT_KEY", "DEFAULT_VALUE"));

    this.sqlLifecycleFactory = new SqlLifecycleFactory(
        plannerFactory,
        serviceEmitter,
        requestLogger,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new AuthConfig(),
        Suppliers.ofInstance(defaultQueryConfig)
    );
  }

  @Test
  public void testIgnoredQueryContextParametersAreIgnored()
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "select 1 + ?";
    final Map<String, Object> queryContext = ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, "true");
    lifecycle.initialize(sql, new QueryContext(queryContext));
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    Assert.assertEquals(2, lifecycle.getQueryContext().getMergedParams().size());
    // should contain only query id, not bySegment since it is not valid for SQL
    Assert.assertTrue(lifecycle.getQueryContext().getMergedParams().containsKey(PlannerContext.CTX_SQL_QUERY_ID));
  }

  @Test
  public void testDefaultQueryContextIsApplied()
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    // lifecycle should not have a query context is there on it when created/factorized
    Assert.assertNull(lifecycle.getQueryContext());
    final String sql = "select 1 + ?";
    final Map<String, Object> queryContext = ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, "true");
    QueryContext testQueryContext = new QueryContext(queryContext);
    // default query context isn't applied to query context until lifecycle is initialized
    for (String defaultContextKey : defaultQueryConfig.getContext().keySet()) {
      Assert.assertFalse(testQueryContext.getMergedParams().containsKey(defaultContextKey));
    }
    lifecycle.initialize(sql, testQueryContext);
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    Assert.assertEquals(2, lifecycle.getQueryContext().getMergedParams().size());
    // should lifecycle should contain default query context values after initialization
    for (String defaultContextKey : defaultQueryConfig.getContext().keySet()) {
      Assert.assertTrue(lifecycle.getQueryContext().getMergedParams().containsKey(defaultContextKey));
    }
  }

  @Test
  public void testStateTransition()
      throws ValidationException, SqlParseException, RelConversionException, IOException
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "select 1 + ?";
    Assert.assertEquals(SqlLifecycle.State.NEW, lifecycle.getState());

    // test initialize
    lifecycle.initialize(sql, new QueryContext());
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    List<TypedValue> parameters = ImmutableList.of(new SqlParameter(SqlType.BIGINT, 1L).getTypedValue());
    lifecycle.setParameters(parameters);
    // setting parameters should not change the state
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());

    // test authorization
    DruidPlanner mockPlanner = EasyMock.createMock(DruidPlanner.class);
    PlannerContext mockPlannerContext = EasyMock.createMock(PlannerContext.class);
    EasyMock.expect(plannerFactory.createPlanner(EasyMock.eq(sql), EasyMock.anyObject())).andReturn(mockPlanner).once();
    EasyMock.expect(mockPlanner.getPlannerContext()).andReturn(mockPlannerContext).once();
    mockPlannerContext.setAuthenticationResult(CalciteTests.REGULAR_USER_AUTH_RESULT);
    EasyMock.expectLastCall();
    mockPlannerContext.setParameters(parameters);
    EasyMock.expectLastCall();
    mockPlanner.validate();
    EasyMock.expectLastCall();
    Set<ResourceAction> mockActions = new HashSet<>();
    mockActions.add(new ResourceAction(new Resource("dummy", ResourceType.DATASOURCE), Action.READ));
    EasyMock.expect(mockPlanner.resourceActions(EasyMock.eq(false))).andReturn(mockActions).once();
    EasyMock.expectLastCall();
    EasyMock.expect(mockPlanner.authorize(EasyMock.anyObject(), EasyMock.eq(false))).andReturn(Access.OK).once();
    EasyMock.expectLastCall();

    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext);

    lifecycle.validateAndAuthorize(CalciteTests.REGULAR_USER_AUTH_RESULT);
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext);

    // test prepare
    PrepareResult mockPrepareResult = EasyMock.createMock(PrepareResult.class);
    EasyMock.expect(mockPlanner.prepare()).andReturn(mockPrepareResult).once();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    lifecycle.prepare();
    // prepare doens't change lifecycle state
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);

    // test plan
    PlannerResult mockPlanResult = EasyMock.createMock(PlannerResult.class);
    EasyMock.expect(mockPlanner.plan()).andReturn(mockPlanResult).once();
    mockPlanner.close();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    lifecycle.plan();
    Assert.assertEquals(mockPlannerContext, lifecycle.getPlannerContext());
    Assert.assertEquals(SqlLifecycle.State.PLANNED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);

    // test execute
    EasyMock.expect(mockPlanResult.run()).andReturn(Sequences.simple(ImmutableList.of(new Object[]{2L}))).once();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    lifecycle.execute();
    Assert.assertEquals(SqlLifecycle.State.EXECUTING, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);

    // test emit
    EasyMock.expect(mockPlannerContext.getSqlQueryId()).andReturn("id").once();
    CopyOnWriteArrayList<String> nativeQueryIds = new CopyOnWriteArrayList<>(ImmutableList.of("id"));
    EasyMock.expect(mockPlannerContext.getNativeQueryIds()).andReturn(nativeQueryIds).times(2);
    EasyMock.expect(mockPlannerContext.getAuthenticationResult()).andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT).once();

    serviceEmitter.emit(EasyMock.anyObject(ServiceEventBuilder.class));
    EasyMock.expectLastCall();
    serviceEmitter.emit(EasyMock.anyObject(ServiceEventBuilder.class));
    EasyMock.expectLastCall();
    requestLogger.logSqlQuery(EasyMock.anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);

    lifecycle.finalizeStateAndEmitLogsAndMetrics(null, null, 10);
    Assert.assertEquals(SqlLifecycle.State.DONE, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
  }

  @Test
  public void testStateTransitionHttpRequest()
      throws ValidationException, SqlParseException, RelConversionException, IOException
  {
    // this test is a duplicate of testStateTransition except with a slight
    // variation of how validate and authorize is run
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "select 1 + ?";
    Assert.assertEquals(SqlLifecycle.State.NEW, lifecycle.getState());

    // test initialize
    lifecycle.initialize(sql, new QueryContext());
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    List<TypedValue> parameters = ImmutableList.of(new SqlParameter(SqlType.BIGINT, 1L).getTypedValue());
    lifecycle.setParameters(parameters);
    // setting parameters should not change the state
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());

    // test authorization
    DruidPlanner mockPlanner = EasyMock.createMock(DruidPlanner.class);
    PlannerContext mockPlannerContext = EasyMock.createMock(PlannerContext.class);
    EasyMock.expect(plannerFactory.createPlanner(EasyMock.eq(sql), EasyMock.anyObject())).andReturn(mockPlanner).once();
    EasyMock.expect(mockPlanner.getPlannerContext()).andReturn(mockPlannerContext).once();
    mockPlannerContext.setAuthenticationResult(CalciteTests.REGULAR_USER_AUTH_RESULT);
    EasyMock.expectLastCall();
    mockPlannerContext.setParameters(parameters);
    EasyMock.expectLastCall();
    mockPlanner.validate();
    EasyMock.expectLastCall();
    Set<ResourceAction> mockActions = new HashSet<>();
    mockActions.add(new ResourceAction(new Resource("dummy", ResourceType.DATASOURCE), Action.READ));
    EasyMock.expect(mockPlanner.resourceActions(EasyMock.eq(false))).andReturn(mockActions).once();
    EasyMock.expectLastCall();
    EasyMock.expect(mockPlanner.authorize(EasyMock.anyObject(), EasyMock.eq(false))).andReturn(Access.OK).once();
    EasyMock.expectLastCall();

    // Note: can't check the request usage with mocks: the code is run
    // in a function which the mock doesn't actually call.
    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT).once();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, request);

    lifecycle.validateAndAuthorize(request);
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, request);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, request);

    // test prepare
    PrepareResult mockPrepareResult = EasyMock.createMock(PrepareResult.class);
    EasyMock.expect(mockPlanner.prepare()).andReturn(mockPrepareResult).once();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    lifecycle.prepare();
    // prepare doens't change lifecycle state
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);

    // test plan
    PlannerResult mockPlanResult = EasyMock.createMock(PlannerResult.class);
    EasyMock.expect(mockPlanner.plan()).andReturn(mockPlanResult).once();
    mockPlanner.close();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    lifecycle.plan();
    Assert.assertEquals(mockPlannerContext, lifecycle.getPlannerContext());
    Assert.assertEquals(SqlLifecycle.State.PLANNED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);

    // test execute
    EasyMock.expect(mockPlanResult.run()).andReturn(Sequences.simple(ImmutableList.of(new Object[]{2L}))).once();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    lifecycle.execute();
    Assert.assertEquals(SqlLifecycle.State.EXECUTING, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);

    // test emit
    EasyMock.expect(mockPlannerContext.getSqlQueryId()).andReturn("id").once();
    CopyOnWriteArrayList<String> nativeQueryIds = new CopyOnWriteArrayList<>(ImmutableList.of("id"));
    EasyMock.expect(mockPlannerContext.getNativeQueryIds()).andReturn(nativeQueryIds).times(2);
    EasyMock.expect(mockPlannerContext.getAuthenticationResult()).andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT).once();

    serviceEmitter.emit(EasyMock.anyObject(ServiceEventBuilder.class));
    EasyMock.expectLastCall();
    serviceEmitter.emit(EasyMock.anyObject(ServiceEventBuilder.class));
    EasyMock.expectLastCall();
    requestLogger.logSqlQuery(EasyMock.anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);

    lifecycle.finalizeStateAndEmitLogsAndMetrics(null, null, 10);
    Assert.assertEquals(SqlLifecycle.State.DONE, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult, mockPlanResult);
  }
}
