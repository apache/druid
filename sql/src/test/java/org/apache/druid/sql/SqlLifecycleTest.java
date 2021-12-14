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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.planner.ValidationResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlParameter;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SqlLifecycleTest
{
  private PlannerFactory plannerFactory;
  private ServiceEmitter serviceEmitter;
  private RequestLogger requestLogger;
  private SqlLifecycleFactory sqlLifecycleFactory;

  @Before
  public void setup()
  {
    this.plannerFactory = EasyMock.createMock(PlannerFactory.class);
    this.serviceEmitter = EasyMock.createMock(ServiceEmitter.class);
    this.requestLogger = EasyMock.createMock(RequestLogger.class);
    this.sqlLifecycleFactory = new SqlLifecycleFactory(
        plannerFactory,
        serviceEmitter,
        requestLogger,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER
    );
  }

  @Test
  public void testIgnoredQueryContextParametersAreIgnored()
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "select 1 + ?";
    final Map<String, Object> queryContext = ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, "true");
    lifecycle.initialize(sql, queryContext);
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    Assert.assertEquals(1, lifecycle.getQueryContext().size());
    // should contain only query id, not bySegment since it is not valid for SQL
    Assert.assertTrue(lifecycle.getQueryContext().containsKey(PlannerContext.CTX_SQL_QUERY_ID));
  }

  @Test
  public void testStateTransition()
      throws ValidationException, SqlParseException, RelConversionException, IOException
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "select 1 + ?";
    final Map<String, Object> queryContext = Collections.emptyMap();
    Assert.assertEquals(SqlLifecycle.State.NEW, lifecycle.getState());

    // test initialize
    lifecycle.initialize(sql, queryContext);
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    List<TypedValue> parameters = ImmutableList.of(new SqlParameter(SqlType.BIGINT, 1L).getTypedValue());
    lifecycle.setParameters(parameters);
    // setting parameters should not change the state
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());

    // test authorization
    DruidPlanner mockPlanner = EasyMock.createMock(DruidPlanner.class);
    PlannerContext mockPlannerContext = EasyMock.createMock(PlannerContext.class);
    ValidationResult validationResult = new ValidationResult(Collections.emptySet());
    EasyMock.expect(plannerFactory.createPlanner(EasyMock.eq(sql), EasyMock.anyObject())).andReturn(mockPlanner).once();
    EasyMock.expect(mockPlanner.getPlannerContext()).andReturn(mockPlannerContext).once();
    mockPlannerContext.setAuthenticationResult(CalciteTests.REGULAR_USER_AUTH_RESULT);
    EasyMock.expectLastCall();
    mockPlannerContext.setParameters(parameters);
    EasyMock.expectLastCall();
    EasyMock.expect(plannerFactory.getAuthorizerMapper()).andReturn(CalciteTests.TEST_AUTHORIZER_MAPPER).once();
    mockPlannerContext.setAuthorizationResult(Access.OK);
    EasyMock.expectLastCall();
    EasyMock.expect(mockPlanner.validate()).andReturn(validationResult).once();
    mockPlanner.close();
    EasyMock.expectLastCall();

    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext);

    lifecycle.validateAndAuthorize(CalciteTests.REGULAR_USER_AUTH_RESULT);
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext);

    // test prepare
    PrepareResult mockPrepareResult = EasyMock.createMock(PrepareResult.class);
    EasyMock.expect(plannerFactory.createPlannerWithContext(EasyMock.eq(mockPlannerContext))).andReturn(mockPlanner).once();
    EasyMock.expect(mockPlanner.prepare()).andReturn(mockPrepareResult).once();
    mockPlanner.close();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    lifecycle.prepare();
    // prepare doens't change lifecycle state
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);

    // test plan
    PlannerResult mockPlanResult = EasyMock.createMock(PlannerResult.class);
    EasyMock.expect(plannerFactory.createPlannerWithContext(EasyMock.eq(mockPlannerContext))).andReturn(mockPlanner).once();
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
    // this test is a duplicate of testStateTransition except with a slight variation of how validate and authorize
    // is run
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "select 1 + ?";
    final Map<String, Object> queryContext = Collections.emptyMap();
    Assert.assertEquals(SqlLifecycle.State.NEW, lifecycle.getState());

    // test initialize
    lifecycle.initialize(sql, queryContext);
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());
    List<TypedValue> parameters = ImmutableList.of(new SqlParameter(SqlType.BIGINT, 1L).getTypedValue());
    lifecycle.setParameters(parameters);
    // setting parameters should not change the state
    Assert.assertEquals(SqlLifecycle.State.INITIALIZED, lifecycle.getState());

    // test authorization
    DruidPlanner mockPlanner = EasyMock.createMock(DruidPlanner.class);
    PlannerContext mockPlannerContext = EasyMock.createMock(PlannerContext.class);
    ValidationResult validationResult = new ValidationResult(Collections.emptySet());
    EasyMock.expect(plannerFactory.createPlanner(EasyMock.eq(sql), EasyMock.anyObject())).andReturn(mockPlanner).once();
    EasyMock.expect(mockPlanner.getPlannerContext()).andReturn(mockPlannerContext).once();
    mockPlannerContext.setAuthenticationResult(CalciteTests.REGULAR_USER_AUTH_RESULT);
    EasyMock.expectLastCall();
    mockPlannerContext.setParameters(parameters);
    EasyMock.expectLastCall();
    EasyMock.expect(plannerFactory.getAuthorizerMapper()).andReturn(CalciteTests.TEST_AUTHORIZER_MAPPER).once();
    mockPlannerContext.setAuthorizationResult(Access.OK);
    EasyMock.expectLastCall();
    EasyMock.expect(mockPlanner.validate()).andReturn(validationResult).once();
    mockPlanner.close();
    EasyMock.expectLastCall();

    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT).times(2);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, request);

    lifecycle.validateAndAuthorize(request);
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, request);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, request);

    // test prepare
    PrepareResult mockPrepareResult = EasyMock.createMock(PrepareResult.class);
    EasyMock.expect(plannerFactory.createPlannerWithContext(EasyMock.eq(mockPlannerContext))).andReturn(mockPlanner).once();
    EasyMock.expect(mockPlanner.prepare()).andReturn(mockPrepareResult).once();
    mockPlanner.close();
    EasyMock.expectLastCall();
    EasyMock.replay(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    lifecycle.prepare();
    // prepare doens't change lifecycle state
    Assert.assertEquals(SqlLifecycle.State.AUTHORIZED, lifecycle.getState());
    EasyMock.verify(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);
    EasyMock.reset(plannerFactory, serviceEmitter, requestLogger, mockPlanner, mockPlannerContext, mockPrepareResult);

    // test plan
    PlannerResult mockPlanResult = EasyMock.createMock(PlannerResult.class);
    EasyMock.expect(plannerFactory.createPlannerWithContext(EasyMock.eq(mockPlannerContext))).andReturn(mockPlanner).once();
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
