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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Druid SQL planner. Wraps the underlying Calcite planner with Druid-specific
 * actions around resource validation and conversion of the Calcite logical
 * plan into a Druid native query.
 * <p>
 * The planner is designed to use once: it makes one trip through its
 * lifecycle defined as:
 * <p>
 * start --> validate [--> prepare] --> plan
 */
public class DruidPlanner implements Closeable
{
  public enum State
  {
    START, VALIDATED, PREPARED, PLANNED
  }

  private final FrameworkConfig frameworkConfig;
  private final CalcitePlanner planner;
  private final PlannerContext plannerContext;
  private final SqlEngine engine;
  private State state = State.START;
  private SqlStatementHandler handler;
  private boolean authorized;

  DruidPlanner(
      final FrameworkConfig frameworkConfig,
      final PlannerContext plannerContext,
      final SqlEngine engine
  )
  {
    this.frameworkConfig = frameworkConfig;
    this.planner = new CalcitePlanner(frameworkConfig);
    this.plannerContext = plannerContext;
    this.engine = engine;
  }

  /**
   * Validates a SQL query and populates {@link PlannerContext#getResourceActions()}.
   *
   * @return set of {@link Resource} corresponding to any Druid datasources
   * or views which are taking part in the query.
   */
  public void validate() throws SqlParseException, ValidationException
  {
    Preconditions.checkState(state == State.START);

    // Validate query context.
    engine.validateContext(plannerContext.getQueryContext());

    // Parse the query string.
    SqlNode root = planner.parse(plannerContext.getSql());
    handler = createHandler(root);

    try {
      handler.validate();
      plannerContext.setResourceActions(handler.resourceActions());
    }
    catch (RuntimeException e) {
      throw new ValidationException(e);
    }

    state = State.VALIDATED;
  }

  private SqlStatementHandler createHandler(final SqlNode node) throws ValidationException
  {
    SqlNode query = node;
    SqlExplain explain = null;
    if (query.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) query;
      query = explain.getExplicandum();
    }

    SqlStatementHandler.HandlerContext handlerContext = new HandlerContextImpl();
    if (query.getKind() == SqlKind.INSERT) {
      if (query instanceof DruidSqlInsert) {
        return new IngestHandler.InsertHandler(handlerContext, (DruidSqlInsert) query, explain);
      } else if (query instanceof DruidSqlReplace) {
        return new IngestHandler.ReplaceHandler(handlerContext, (DruidSqlReplace) query, explain);
      }
    }

    if (query.isA(SqlKind.QUERY)) {
      return new QueryHandler.SelectHandler(handlerContext, query, explain);
    }
    throw new ValidationException(StringUtils.format("Cannot execute [%s].", node.getKind()));
  }


  /**
   * Prepare a SQL query for execution, including some initial parsing and
   * validation and any dynamic parameter type resolution, to support prepared
   * statements via JDBC.
   *
   * Prepare reuses the validation done in {@link #validate()} which must be
   * called first.
   *
   * A query can be prepared on a data source without having permissions on
   * that data source. This odd state of affairs is necessary because
   * {@link org.apache.druid.sql.calcite.view.DruidViewMacro} prepares
   * a view while having no information about the user of that view.
   */
  public PrepareResult prepare()
  {
    Preconditions.checkState(state == State.VALIDATED);
    handler.prepare();
    state = State.PREPARED;
    return prepareResult();
  }

  /**
   * Authorizes the statement. Done within the planner to enforce the authorization
   * step within the planner's state machine.
   *
   * @param authorizer a function from resource actions to a {@link Access} result.
   *
   * @return the return value from the authorizer
   */
  public Access authorize(Function<Set<ResourceAction>, Access> authorizer, boolean authorizeContextParams)
  {
    Preconditions.checkState(state == State.VALIDATED);
    Access access = authorizer.apply(resourceActions(authorizeContextParams));
    plannerContext.setAuthorizationResult(access);

    // Authorization is done as a flag, not a state, alas.
    // Views do prepare without authorize, Avatica does authorize, then prepare,
    // so the only constraint is that authorize be done after validation, before plan.
    authorized = true;
    return access;
  }

  /**
   * Return the resource actions corresponding to the datasources and views which
   * an authenticated request must be authorized for to process the query. The
   * actions will be {@code null} if the planner has not yet advanced to the
   * validation step. This may occur if validation fails and the caller accesses
   * the resource actions as part of clean-up.
   */
  public Set<ResourceAction> resourceActions(boolean includeContext)
  {
    Set<ResourceAction> resourceActions = plannerContext.getResourceActions();
    if (includeContext) {
      Set<ResourceAction> actions = new HashSet<>(resourceActions);
      plannerContext.getQueryContext().getUserParams().keySet().forEach(contextParam -> actions.add(
          new ResourceAction(new Resource(contextParam, ResourceType.QUERY_CONTEXT), Action.WRITE)
      ));
      return actions;
    } else {
      return resourceActions;
    }
  }

  /**
   * Plan an SQL query for execution, returning a {@link PlannerResult} which can be used to actually execute the query.
   * <p>
   * Ideally, the query can be planned into a native Druid query, but will
   * fall-back to bindable convention if this is not possible.
   * <p>
   * Planning reuses the validation done in {@code validate()} which must be called first.
   */
  public PlannerResult plan() throws ValidationException
  {
    Preconditions.checkState(state == State.VALIDATED || state == State.PREPARED);
    Preconditions.checkState(authorized);
    state = State.PLANNED;
    return handler.plan();
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public PrepareResult prepareResult()
  {
    return handler.prepareResult();
  }

  @Override
  public void close()
  {
    planner.close();
  }

  protected class HandlerContextImpl implements SqlStatementHandler.HandlerContext
  {
    @Override
    public PlannerContext plannerContext()
    {
      return plannerContext;
    }

    @Override
    public SqlEngine engine()
    {
      return engine;
    }

    @Override
    public CalcitePlanner planner()
    {
      return planner;
    }

    @Override
    public QueryContext queryContext()
    {
      return plannerContext.getQueryContext();
    }

    @Override
    public SchemaPlus defaultSchema()
    {
      return frameworkConfig.getDefaultSchema();
    }

    @Override
    public ObjectMapper jsonMapper()
    {
      return plannerContext.getJsonMapper();
    }

    @Override
    public DateTimeZone timeZone()
    {
      return plannerContext.getTimeZone();
    }
  }
}
