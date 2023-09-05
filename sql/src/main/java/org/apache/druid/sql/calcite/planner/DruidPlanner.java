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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.parser.ParseException;
import org.apache.druid.sql.calcite.parser.Token;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
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

  public static final Joiner SPACE_JOINER = Joiner.on(" ");
  public static final Joiner COMMA_JOINER = Joiner.on(", ");

  public enum State
  {
    START, VALIDATED, PREPARED, PLANNED
  }

  public static class AuthResult
  {
    public final Access authorizationResult;

    /**
     * Resource actions used with authorizing a cancellation request. These actions
     * include only the data-level actions (e.g. the datasource.)
     */
    public final Set<ResourceAction> sqlResourceActions;

    /**
     * Full resource actions authorized as part of this request. Used when logging
     * resource actions. Includes query context keys, if query context authorization
     * is enabled.
     */
    public final Set<ResourceAction> allResourceActions;

    public AuthResult(
        final Access authorizationResult,
        final Set<ResourceAction> sqlResourceActions,
        final Set<ResourceAction> allResourceActions
    )
    {
      this.authorizationResult = authorizationResult;
      this.sqlResourceActions = sqlResourceActions;
      this.allResourceActions = allResourceActions;
    }
  }

  private final FrameworkConfig frameworkConfig;
  private final CalcitePlanner planner;
  private final PlannerContext plannerContext;
  private final SqlEngine engine;
  private final PlannerHook hook;
  private State state = State.START;
  private SqlStatementHandler handler;
  private boolean authorized;

  DruidPlanner(
      final FrameworkConfig frameworkConfig,
      final PlannerContext plannerContext,
      final SqlEngine engine,
      final PlannerHook hook
  )
  {
    this.frameworkConfig = frameworkConfig;
    this.planner = new CalcitePlanner(frameworkConfig);
    this.plannerContext = plannerContext;
    this.engine = engine;
    this.hook = hook == null ? NoOpPlannerHook.INSTANCE : hook;
  }

  /**
   * Validates a SQL query and populates {@link PlannerContext#getResourceActions()}.
   *
   * @return set of {@link Resource} corresponding to any Druid datasources
   * or views which are taking part in the query.
   */
  public void validate()
  {
    Preconditions.checkState(state == State.START);

    // Validate query context.
    engine.validateContext(plannerContext.queryContextMap());

    // Parse the query string.
    String sql = plannerContext.getSql();
    hook.captureSql(sql);
    SqlNode root;
    try {
      root = planner.parse(sql);
    }
    catch (SqlParseException e1) {
      throw translateException(e1);
    }
    handler = createHandler(root);
    handler.validate();
    plannerContext.setResourceActions(handler.resourceActions());
    state = State.VALIDATED;
  }

  private SqlStatementHandler createHandler(final SqlNode node)
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
    throw InvalidSqlInput.exception("Unsupported SQL statement [%s]", node.getKind());
  }

  /**
   * Prepare a SQL query for execution, including some initial parsing and
   * validation and any dynamic parameter type resolution, to support prepared
   * statements via JDBC.
   * <p>
   * Prepare reuses the validation done in {@link #validate()} which must be
   * called first.
   * <p>
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
   * @param authorizer   a function from resource actions to a {@link Access} result.
   * @param extraActions set of additional resource actions beyond those inferred
   *                     from the query itself. Specifically, the set of context keys to
   *                     authorize.
   * @return the return value from the authorizer
   */
  public AuthResult authorize(
      final Function<Set<ResourceAction>, Access> authorizer,
      final Set<ResourceAction> extraActions
  )
  {
    Preconditions.checkState(state == State.VALIDATED);
    Set<ResourceAction> sqlResourceActions = plannerContext.getResourceActions();
    Set<ResourceAction> allResourceActions = new HashSet<>(sqlResourceActions);
    allResourceActions.addAll(extraActions);
    Access access = authorizer.apply(allResourceActions);
    plannerContext.setAuthorizationResult(access);

    // Authorization is done as a flag, not a state, alas.
    // Views prepare without authorization, Avatica does authorize, then prepare,
    // so the only constraint is that authorization be done before planning.
    authorized = true;
    return new AuthResult(access, sqlResourceActions, allResourceActions);
  }

  /**
   * Plan an SQL query for execution, returning a {@link PlannerResult} which can be used to actually execute the query.
   * <p>
   * Ideally, the query can be planned into a native Druid query, but will
   * fall-back to bindable convention if this is not possible.
   * <p>
   * Planning reuses the validation done in {@code validate()} which must be called first.
   */
  public PlannerResult plan()
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
      return plannerContext.queryContext();
    }

    @Override
    public Map<String, Object> queryContextMap()
    {
      return plannerContext.queryContextMap();
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

    @Override
    public PlannerHook hook()
    {
      return hook;
    }
  }

  public static DruidException translateException(Exception e)
  {
    try {
      throw e;
    }
    catch (DruidException inner) {
      return inner;
    }
    catch (ValidationException inner) {
      return parseValidationMessage(inner);
    }
    catch (SqlParseException inner) {
      final Throwable cause = inner.getCause();
      if (cause instanceof DruidException) {
        return (DruidException) cause;
      }

      if (cause instanceof ParseException) {
        ParseException parseException = (ParseException) cause;
        final SqlParserPos failurePosition = inner.getPos();
        // When calcite catches a syntax error at the top level
        // expected token sequences can be null.
        // In such a case return the syntax error to the user
        // wrapped in a DruidException with invalid input
        if (parseException.expectedTokenSequences == null) {
          return DruidException.forPersona(DruidException.Persona.USER)
                               .ofCategory(DruidException.Category.INVALID_INPUT)
                               .withErrorCode("invalidInput")
                               .build(inner, inner.getMessage()).withContext("sourceType", "sql");
        } else {
          final String theUnexpectedToken = getUnexpectedTokenString(parseException);

          final String[] tokenDictionary = inner.getTokenImages();
          final int[][] expectedTokenSequences = inner.getExpectedTokenSequences();
          final ArrayList<String> expectedTokens = new ArrayList<>(expectedTokenSequences.length);
          for (int[] expectedTokenSequence : expectedTokenSequences) {
            String[] strings = new String[expectedTokenSequence.length];
            for (int i = 0; i < expectedTokenSequence.length; ++i) {
              strings[i] = tokenDictionary[expectedTokenSequence[i]];
            }
            expectedTokens.add(SPACE_JOINER.join(strings));
          }

          return InvalidSqlInput
              .exception(
                  inner,
                  "Received an unexpected token [%s] (line [%s], column [%s]), acceptable options: [%s]",
                  theUnexpectedToken,
                  failurePosition.getLineNum(),
                  failurePosition.getColumnNum(),
                  COMMA_JOINER.join(expectedTokens)
              )
              .withContext("line", failurePosition.getLineNum())
              .withContext("column", failurePosition.getColumnNum())
              .withContext("endLine", failurePosition.getEndLineNum())
              .withContext("endColumn", failurePosition.getEndColumnNum())
              .withContext("token", theUnexpectedToken)
              .withContext("expected", expectedTokens);

        }
      }

      return DruidException.forPersona(DruidException.Persona.DEVELOPER)
                           .ofCategory(DruidException.Category.UNCATEGORIZED)
                           .build(
                               inner,
                               "Unable to parse the SQL, unrecognized error from calcite: [%s]",
                               inner.getMessage()
                           );
    }
    catch (RelOptPlanner.CannotPlanException inner) {
      return DruidException.forPersona(DruidException.Persona.USER)
                           .ofCategory(DruidException.Category.INVALID_INPUT)
                           .build(inner, inner.getMessage());
    }
    catch (Exception inner) {
      // Anything else. Should not get here. Anything else should already have
      // been translated to a DruidException unless it is an unexpected exception.
      return DruidException.forPersona(DruidException.Persona.ADMIN)
                           .ofCategory(DruidException.Category.UNCATEGORIZED)
                           .build(inner, inner.getMessage());
    }
  }

  private static DruidException parseValidationMessage(Exception e)
  {
    if (e.getCause() instanceof DruidException) {
      return (DruidException) e.getCause();
    }

    Throwable maybeContextException = e;
    CalciteContextException contextException = null;
    while (maybeContextException != null) {
      if (maybeContextException instanceof CalciteContextException) {
        contextException = (CalciteContextException) maybeContextException;
        break;
      }
      maybeContextException = maybeContextException.getCause();
    }

    if (contextException != null) {
      return InvalidSqlInput
          .exception(
              e,
              "%s (line [%s], column [%s])",
              // the CalciteContextException .getMessage() assumes cause is non-null, so this should be fine
              contextException.getCause().getMessage(),
              contextException.getPosLine(),
              contextException.getPosColumn()
          )
          .withContext("line", String.valueOf(contextException.getPosLine()))
          .withContext("column", String.valueOf(contextException.getPosColumn()))
          .withContext("endLine", String.valueOf(contextException.getEndPosLine()))
          .withContext("endColumn", String.valueOf(contextException.getEndPosColumn()));
    } else {
      return DruidException.forPersona(DruidException.Persona.USER)
                           .ofCategory(DruidException.Category.UNCATEGORIZED)
                           .build(e, "Uncategorized calcite error message: [%s]", e.getMessage());
    }
  }

  /**
   * Grabs the unexpected token string.  This code is borrowed with minimal adjustments from
   * {@link ParseException#getMessage()}.  It is possible that if that code changes, we need to also
   * change this code to match it.
   *
   * @param parseException the parse exception to extract from
   * @return the String representation of the unexpected token string
   */
  private static String getUnexpectedTokenString(ParseException parseException)
  {
    int maxSize = 0;
    for (int[] ints : parseException.expectedTokenSequences) {
      if (maxSize < ints.length) {
        maxSize = ints.length;
      }
    }

    StringBuilder bob = new StringBuilder();
    Token tok = parseException.currentToken.next;
    for (int i = 0; i < maxSize; i++) {
      if (i != 0) {
        bob.append(" ");
      }
      if (tok.kind == 0) {
        bob.append("<EOF>");
        break;
      }
      char ch;
      for (int i1 = 0; i1 < tok.image.length(); i1++) {
        switch (tok.image.charAt(i1)) {
          case 0:
            continue;
          case '\b':
            bob.append("\\b");
            continue;
          case '\t':
            bob.append("\\t");
            continue;
          case '\n':
            bob.append("\\n");
            continue;
          case '\f':
            bob.append("\\f");
            continue;
          case '\r':
            bob.append("\\r");
            continue;
          case '\"':
            bob.append("\\\"");
            continue;
          case '\'':
            bob.append("\\\'");
            continue;
          case '\\':
            bob.append("\\\\");
            continue;
          default:
            if ((ch = tok.image.charAt(i1)) < 0x20 || ch > 0x7e) {
              String s = "0000" + Integer.toString(ch, 16);
              bob.append("\\u").append(s.substring(s.length() - 4, s.length()));
            } else {
              bob.append(ch);
            }
            continue;
        }
      }
      tok = tok.next;
    }
    return bob.toString();
  }

}
