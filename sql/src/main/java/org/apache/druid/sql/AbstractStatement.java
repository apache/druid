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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * Represents a SQL statement either for preparation or execution.
 * A statement is given by a lifecycle context and the statement
 * to execute. See derived classes for actions. Closing the statement
 * emits logs and metrics for the statement.
 * <p>
 * The query context has a complex lifecycle. It is provided in the SQL query
 * request ({@link SqlQueryPlus}), then modified during planning. The
 * user-provided copy is immutable: a copy is made in this class, and that
 * copy is the one which the planner adjusts as planning proceeds. Context
 * authorization, if enabled, is done based on the user-provided context keys,
 * not the internally-defined context.
 */
public abstract class AbstractStatement implements Closeable
{
  private static final Logger log = new Logger(AbstractStatement.class);

  protected final SqlToolbox sqlToolbox;
  protected final SqlQueryPlus queryPlus;
  protected final SqlExecutionReporter reporter;

  /**
   * Copy of the query context provided by the user. This copy is modified during
   * planning. Modifications are possible up to the point where the context is passed
   * to a native query. At that point, the context becomes immutable and can be changed
   * only by copying the entire native query.
   */
  protected final Map<String, Object> queryContext;
  protected PlannerContext plannerContext;
  protected DruidPlanner.AuthResult authResult;

  public AbstractStatement(
      final SqlToolbox sqlToolbox,
      final SqlQueryPlus queryPlus,
      final String remoteAddress
  )
  {
    this.sqlToolbox = sqlToolbox;
    this.reporter = new SqlExecutionReporter(this, remoteAddress);
    this.queryPlus = queryPlus;
    this.queryContext = new HashMap<>(queryPlus.context());

    // "bySegment" results are never valid to use with SQL because the result format is incompatible
    // so, overwrite any user specified context to avoid exceptions down the line

    if (this.queryContext.remove(QueryContexts.BY_SEGMENT_KEY) != null) {
      log.warn("'bySegment' results are not supported for SQL queries, ignoring query context parameter");
    }
    this.queryContext.putIfAbsent(QueryContexts.CTX_SQL_QUERY_ID, UUID.randomUUID().toString());
    for (Map.Entry<String, Object> entry : sqlToolbox.defaultQueryConfig.getContext().entrySet()) {
      this.queryContext.putIfAbsent(entry.getKey(), entry.getValue());
    }
  }

  public String sqlQueryId()
  {
    return QueryContexts.parseString(queryContext, QueryContexts.CTX_SQL_QUERY_ID);
  }

  /**
   * Returns the context as it evolves during planning. In general, this copy <i>will not</i>
   * be the same as the one from {@code getQuery().context()}.
   */
  public Map<String, Object> context()
  {
    return queryContext;
  }

  /**
   * Validate SQL query and authorize against any datasources or views which
   * will take part in the query. Must be called by the API methods, not
   * directly.
   */
  protected void validate(final DruidPlanner planner)
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
      final DruidPlanner planner,
      final Function<Set<ResourceAction>, Access> authorizer
  )
  {
    Set<String> securedKeys = this.sqlToolbox.authConfig.contextKeysToAuthorize(queryPlus.context().keySet());
    Set<ResourceAction> contextResources = new HashSet<>();
    securedKeys.forEach(key -> contextResources.add(
        new ResourceAction(new Resource(key, ResourceType.QUERY_CONTEXT), Action.WRITE)
    ));

    // Authentication is done by the planner using the function provided
    // here. The planner ensures that this step is done before planning.
    authResult = planner.authorize(authorizer, contextResources);
    if (!authResult.authorizationResult.isAllowed()) {
      throw new ForbiddenException(authResult.authorizationResult.toMessage());
    }
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
   * Return the datasource and table resources for this
   * statement.
   */
  public Set<ResourceAction> resources()
  {
    return authResult.sqlResourceActions;
  }

  public Set<ResourceAction> allResources()
  {
    return authResult.allResourceActions;
  }

  public SqlQueryPlus query()
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
    try {
      closeQuietly();
    }
    catch (Exception e) {
      reporter.failed(e);
    }
    reporter.emit();
  }

  /**
   * Closes the statement without reporting metrics.
   */
  public void closeQuietly()
  {
  }

  /**
   * Convenience method to close the statement and report an error
   * associated with the statement. Same as calling:{@code
   * stmt.reporter().failed(e);
   * stmt.close();
   * }
   */
  public void closeWithError(Throwable e)
  {
    reporter.failed(e);
    close();
  }
}
