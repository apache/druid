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

package org.apache.druid.sql.calcite.tester;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.QueryDefn;
import org.apache.druid.sql.calcite.planner.CapturedState;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PlannerStateCapture;
import org.apache.druid.sql.calcite.planner.ValidationResult;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Druid SQL query runner. Encapsulates the planner functionality needed
 * to plan and run a query. Provides the ability to introspect the
 * planner details when testing.
 * <p>
 * This class wraps functionality which was previously spread widely
 * in the code, or tightly coupled to a particular representation. This
 * for is usable for both "production" and test code.
 */
public class QueryRunner
{
  /**
   * Builder for the query definition.
   */
  public static class Builder
  {
    private final String sql;
    private Map<String, Object> context;
    private List<SqlParameter> parameters;
    private AuthenticationResult authenticationResult;

    public Builder(String sql)
    {
      this.sql = sql;
    }

    public Builder(SqlQuery query)
    {
      this.sql = query.getQuery();
      this.context = query.getContext();
      this.parameters = query.getParameters();
    }

    public Builder context(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder parameters(List<SqlParameter> parameters)
    {
      this.parameters = parameters;
      return this;
    }

    public Builder authResult(AuthenticationResult authenticationResult)
    {
      this.authenticationResult = authenticationResult;
      return this;
    }

    public QueryDefn build()
    {
      return new QueryDefn(
          sql,
          context == null ? ImmutableMap.of() : context,
          parameters == null ? Collections.emptyList() : parameters,
          authenticationResult);
    }
  }

  /**
   * Introspected planner details, typically for testing.
   */
  public static class PlanDetails
  {
    private final QueryDefn queryDefn;
    private final PlannerResult plannerResult;
    private final CapturedState validateState;
    private final CapturedState planState;

    public PlanDetails(
        QueryDefn queryDefn,
        CapturedState validateState,
        CapturedState planState,
        PlannerResult plannerResult)
    {
      this.queryDefn = queryDefn;
      this.validateState = validateState;
      this.planState = planState;
      this.plannerResult = plannerResult;
    }

    public QueryDefn queryDefn()
    {
      return queryDefn;
    }

    public PlannerResult plannerResult()
    {
      return plannerResult;
    }

    public CapturedState planState()
    {
      return planState;
    }

    public ValidationResult validationResult()
    {
      return validateState.validationResult;
    }
  }

  private final PlannerFactory plannerFactory;
  private final AuthorizerMapper authorizerMapper;

  public QueryRunner(
      PlannerFactory plannerFactory,
      AuthorizerMapper authorizerMapper
  )
  {
    this.plannerFactory = plannerFactory;
    this.authorizerMapper = authorizerMapper;
  }

  /**
   * Run a query and provide the result set.
   */
  public Sequence<Object[]> run(QueryDefn defn) throws SqlParseException, ValidationException, RelConversionException
  {
    return plan(defn).run();
  }

  /**
   * Plan the query and provide the planner details for testing.
   */
  public PlanDetails introspectPlan(QueryDefn defn) throws Exception
  {
    CapturedState validateState = new CapturedState();
    CapturedState planState = new CapturedState();
    PlannerResult plannerResult = plan(defn, validateState, planState);
    return new PlanDetails(defn, validateState, planState, plannerResult);
  }

  /**
   * Plan a query.
   */
  public PlannerResult plan(QueryDefn defn) throws SqlParseException, ValidationException, RelConversionException
  {
    return plan(defn, null, null);
  }

  public PlannerResult plan(
      QueryDefn defn,
      PlannerStateCapture validationCapture,
      PlannerStateCapture planCapture
  ) throws SqlParseException, ValidationException, RelConversionException
  {
    // Oddly, Druid runs the whole parser and conversion twice per query...
    PlannerContext plannerContext;
    try (DruidPlanner planner = plannerFactory.createPlanner(
        defn.sql(),
        new QueryContext(defn.context()))) {
      if (validationCapture != null) {
        planner.captureState(validationCapture);
      }
      plannerContext = planner.getPlannerContext();
      plannerContext.setParameters(defn.typedParameters());
      plannerContext.setAuthenticationResult(defn.authResult());
      ValidationResult validationResult = planner.validate(false);
      Access access =
          AuthorizationUtils.authorizeAllResourceActions(
              defn.authResult(),
              validationResult.getResourceActions(),
              authorizerMapper
          );
      plannerContext.setAuthorizationResult(access);
      if (!access.isAllowed()) {
        throw new ForbiddenException(access.toString());
      }
    }
    try (DruidPlanner planner = plannerFactory.createPlannerWithContext(plannerContext)) {
      if (planCapture != null) {
        planner.captureState(planCapture);
      }
      return planner.plan();
    }
  }
}
