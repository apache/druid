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

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerResult;

import java.io.Closeable;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * Represents a SQL statement either for preparation or execution.
 * A statement is given by a lifecycle context and the statement
 * to execute. See derived classes for actions. Closing the statement
 * emits logs and metrics for the statement.
 */
public abstract class AbstractStatement implements Closeable
{
  private static final Logger log = new Logger(AbstractStatement.class);

  protected final SqlToolbox sqlToolbox;
  protected final SqlQueryPlus queryPlus;
  protected final SqlExecutionReporter reporter;
  protected PlannerContext plannerContext;
  protected Set<ResourceAction> queryResource;
  protected Set<ResourceAction> resourceActions;

  public AbstractStatement(
      final SqlToolbox sqlToolbox,
      final SqlQueryPlus sqlRequest,
      final String remoteAddress
  )
  {
    this.sqlToolbox = sqlToolbox;
    this.queryPlus = sqlRequest;
    this.reporter = new SqlExecutionReporter(this, remoteAddress);

    // Context is modified, not copied.
    contextWithSqlId(sqlRequest.context())
      .addDefaultParams(sqlToolbox.defaultQueryConfig.getContext());
  }

  private QueryContext contextWithSqlId(QueryContext queryContext)
  {
    // "bySegment" results are never valid to use with SQL because the result format is incompatible
    // so, overwrite any user specified context to avoid exceptions down the line

    if (queryContext.removeUserParam(QueryContexts.BY_SEGMENT_KEY) != null) {
      log.warn("'bySegment' results are not supported for SQL queries, ignoring query context parameter");
    }
    queryContext.addDefaultParam(PlannerContext.CTX_SQL_QUERY_ID, UUID.randomUUID().toString());
    return queryContext;
  }

  public String sqlQueryId()
  {
    return queryPlus.context().getAsString(PlannerContext.CTX_SQL_QUERY_ID);
  }

  /**
   * Validate SQL query and authorize against any datasources or views which
   * will take part in the query. Must be called by the API methods, not
   * directly.
   */
  protected void validate(DruidPlanner planner)
  {
    plannerContext = planner.getPlannerContext();
    plannerContext.setAuthenticationResult(queryPlus.authResult());
    plannerContext.setParameters(queryPlus.parameters());
    try {
      planner.validate();
    }
    // We can't collapse catch clauses since SqlPlanningException has
    // type-sensitive constructors.
    catch (SqlParseException e) {
      throw new SqlPlanningException(e);
    }
    catch (ValidationException e) {
      throw new SqlPlanningException(e);
    }
  }

  /**
   * Authorize the query using the authorizer provided, and an option to authorize
   * context variables as well as query resources.
   */
  protected void authorize(
      DruidPlanner planner,
      Function<Set<ResourceAction>, Access> authorizer
  )
  {
    boolean authorizeContextParams = sqlToolbox.authConfig.authorizeQueryContextParams();

    // Authentication is done by the planner using the function provided
    // here. The planner ensures that this step is done before planning.
    Access authorizationResult = planner.authorize(authorizer, authorizeContextParams);
    if (!authorizationResult.isAllowed()) {
      throw new ForbiddenException(authorizationResult.toString());
    }

    queryResource = planner.resourceActions(false);
    resourceActions = planner.resourceActions(authorizeContextParams);
  }

  /**
   * Resource authorizer based on the authentication result
   * provided earlier.
   */
  protected Function<Set<ResourceAction>, Access> authorizer()
  {
    return resourceActions ->
      AuthorizationUtils.authorizeAllResourceActions(
          queryPlus.authResult(),
          resourceActions,
          sqlToolbox.plannerFactory.getAuthorizerMapper()
    );
  }

  /**
   * Plan the query, which also produces the sequence that runs
   * the query.
   */
  protected PlannerResult plan(DruidPlanner planner)
  {
    try {
      return planner.plan();
    }
    catch (ValidationException e) {
      throw new SqlPlanningException(e);
    }
  }

  /**
   * Return the datasource and table resources for this
   * statement.
   */
  public Set<ResourceAction> resources()
  {
    return queryResource;
  }

  public Set<ResourceAction> allResources()
  {
    return resourceActions;
  }

  public SqlQueryPlus sqlRequest()
  {
    return queryPlus;
  }

  public SqlExecutionReporter reporter()
  {
    return reporter;
  }

  /**
   * Releases resources and emits logs and metrics as defined the
   * associated reporter.
   */
  @Override
  public void close()
  {
    closeQuietly();
    reporter.emit();
  }

  /**
   * Closes the statement without reporting metrics.
   */
  public void closeQuietly()
  {
  }
}
