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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.IngestHandler.InsertHandler;
import org.apache.druid.sql.calcite.planner.IngestHandler.ReplaceHandler;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.regex.Pattern;

public class DruidPlanner implements Closeable
{
  public enum State
  {
    START, VALIDATED, PREPARED, PLANNED
  }
  /**
   * SQL-statement-specific behavior. Each statement follows the same
   * lifecycle: analyze, followed by either prepare or plan (execute).
   */
  interface SqlStatementHandler
  {
    void analyze() throws ValidationException;
    Set<ResourceAction> resourceActions();
    PrepareResult prepare() throws RelConversionException, ValidationException;
    PlannerResult plan() throws SqlParseException, ValidationException, RelConversionException;
  }

  static final EmittingLogger log = new EmittingLogger(DruidPlanner.class);
  static final Pattern UNNAMED_COLUMN_PATTERN = Pattern.compile("^EXPR\\$\\d+$", Pattern.CASE_INSENSITIVE);
  @VisibleForTesting
  public static final String UNNAMED_INGESTION_COLUMN_ERROR =
      "Cannot ingest expressions that do not have an alias "
          + "or columns with names like EXPR$[digit].\n"
          + "E.g. if you are ingesting \"func(X)\", then you can rewrite it as "
          + "\"func(X) as myColumn\"";

  private final CalcitePlanner planner;
  private final PlannerContext plannerContext;
  final QueryMakerFactory queryMakerFactory;
  private State state = State.START;
  private SqlStatementHandler handler;

  DruidPlanner(
      final FrameworkConfig frameworkConfig,
      final PlannerContext plannerContext,
      final QueryMakerFactory queryMakerFactory
  )
  {
    this.planner = new CalcitePlanner(frameworkConfig);
    this.plannerContext = plannerContext;
    this.queryMakerFactory = queryMakerFactory;
  }

  private SqlStatementHandler createHandler(SqlNode root) throws ValidationException
  {
    HandlerContext handlerContext = new HandlerContext(plannerContext, planner, queryMakerFactory);
    SqlExplain explain = null;
    if (root.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) root;
      root = explain.getExplicandum();
    }

    if (root.getKind() == SqlKind.INSERT) {
      if (root instanceof DruidSqlInsert) {
        return new InsertHandler(handlerContext, (DruidSqlInsert) root, explain);
      } else if (root instanceof DruidSqlReplace) {
        return new ReplaceHandler(handlerContext, (DruidSqlReplace) root, plannerContext.getTimeZone(), explain);
      }
    }

    if (root.isA(SqlKind.QUERY)) {
      return new SelectHandler(handlerContext, root, explain);
    }

    throw new ValidationException(StringUtils.format("Cannot handle [%s].", root.getKind()));
  }

  /**
   * Validates a SQL query and populates {@link PlannerContext#getResourceActions()}.
   *
   * @return set of {@link Resource} corresponding to any Druid datasources or views which are taking part in the query.
   */
  public ValidationResult validate(boolean authorizeContextParams) throws SqlParseException, ValidationException
  {
    Preconditions.checkState(state == State.START);
    SqlNode root = planner.parse(plannerContext.getSql());
    handler = createHandler(root);
    handler.analyze();

    final Set<ResourceAction> resourceActions = handler.resourceActions();
    if (authorizeContextParams) {
      plannerContext.getQueryContext().getUserParams().keySet().forEach(contextParam -> resourceActions.add(
          new ResourceAction(new Resource(contextParam, ResourceType.QUERY_CONTEXT), Action.WRITE)
      ));
    }

    plannerContext.setResourceActions(resourceActions);
    state = State.VALIDATED;
    return new ValidationResult(resourceActions);
  }

  /**
   * Prepare a SQL query for execution to support prepared statements via JDBC.
   * The statement must have already been validated.
   */
  public PrepareResult prepare() throws SqlParseException, ValidationException, RelConversionException
  {
    Preconditions.checkState(state == State.VALIDATED);
    state = State.PREPARED;
    return handler.prepare();
  }

  /**
   * Plan an SQL query for execution, returning a {@link PlannerResult} which
   * can be used to actually execute the query.
   *
   * Ideally, the query can be planned into a native Druid query, using
   * {@link #planWithDruidConvention}, but will fall-back to
   * {@link #planWithBindableConvention} if this is not possible.
   */
  public PlannerResult plan() throws SqlParseException, ValidationException, RelConversionException
  {
    Preconditions.checkState(state == State.VALIDATED);
    state = State.PLANNED;
    return handler.plan();
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  @Override
  public void close()
  {
    planner.close();
  }
}
