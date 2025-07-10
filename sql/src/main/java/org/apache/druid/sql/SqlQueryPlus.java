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

import com.google.common.base.Preconditions;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.SqlNode;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.parser.StatementAndSetContext;
import org.apache.druid.sql.calcite.planner.CalcitePlanner;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the inputs to a SQL execution request: the statement (as a string),
 * the parsed statement, the context, parameters, and the authorization result.
 * The request can evolve: the context and parameters can be filled in later
 * as needed.
 * <p>
 * SQL requests come from a variety of sources in a variety of formats. Use
 * the {@link Builder} class to create an instance from the information
 * available at each point in the code.
 * <p>
 * Each instance of SqlQueryPlus can only be used once, because {@link #sqlNode}
 * is a mutable data structure, modified during {@link CalcitePlanner#validate}.
 * If you need to use one again, call {@link #freshCopy()} to create a fresh
 * copy with a new {@link SqlNode}.
 * <p>
 * The query context has a complex lifecycle. The copy here is immutable:
 * it is the set of values which the user requested. Planning will
 * add (and sometimes remove) values: that work should be done on a copy of the
 * context so that we have a clean record of the user's original requested
 * values. This original record is required to perform security on the set
 * of user-provided context keys.
 */
public class SqlQueryPlus
{
  private final String sql;
  @Nullable
  private final SqlNode sqlNode;
  private boolean allowSetStatements;
  private final Map<String, Object> queryContext;
  private final List<TypedValue> parameters;
  private final AuthenticationResult authResult;

  private SqlQueryPlus(
      String sql,
      SqlNode sqlNode,
      boolean allowSetStatements,
      Map<String, Object> queryContext,
      List<TypedValue> parameters,
      AuthenticationResult authResult
  )
  {
    this.sql = Preconditions.checkNotNull(sql);
    this.sqlNode = sqlNode;
    this.allowSetStatements = allowSetStatements;
    this.queryContext = queryContext == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new HashMap<>(queryContext));
    this.parameters = parameters == null
                      ? Collections.emptyList()
                      : parameters;
    this.authResult = Preconditions.checkNotNull(authResult);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(String sql)
  {
    return new Builder().sql(sql);
  }

  public String sql()
  {
    return sql;
  }

  public SqlNode sqlNode()
  {
    if (sqlNode == null) {
      throw DruidException.defensive("sqlNode not set");
    }

    return sqlNode;
  }

  public Map<String, Object> context()
  {
    return queryContext;
  }

  public List<TypedValue> parameters()
  {
    return parameters;
  }

  public AuthenticationResult authResult()
  {
    return authResult;
  }

  public SqlQueryPlus withContext(Map<String, Object> context)
  {
    return new SqlQueryPlus(sql, sqlNode, allowSetStatements, context, parameters, authResult);
  }

  public SqlQueryPlus withParameters(List<TypedValue> parameters)
  {
    return new SqlQueryPlus(sql, sqlNode, allowSetStatements, queryContext, parameters, authResult);
  }

  /**
   * Returns a copy of this instance where everything is shared, except the {@link #sqlNode}, which is re-parsed from
   * the SQL statement.
   */
  public SqlQueryPlus freshCopy()
  {
    return new SqlQueryPlus(
        sql,
        DruidSqlParser.parse(sql, allowSetStatements).getMainStatement(),
        allowSetStatements,
        queryContext,
        parameters,
        authResult
    );
  }

  @Override
  public String toString()
  {
    return "SqlQueryPlus{" +
           "sql='" + sql + '\'' +
           ", sqlNode=" + sqlNode +
           ", queryContext=" + queryContext +
           ", parameters=" + parameters +
           ", authResult=" + authResult +
           '}';
  }

  public static class Builder
  {
    private String sql;
    private Map<String, Object> queryContext;
    private List<TypedValue> parameters;
    private AuthenticationResult authResult;

    public Builder sql(String sql)
    {
      this.sql = sql;
      return this;
    }

    public Builder context(Map<String, Object> queryContext)
    {
      this.queryContext = queryContext;
      return this;
    }

    public Builder parameters(List<TypedValue> parameters)
    {
      this.parameters = parameters;
      return this;
    }

    public Builder sqlParameters(List<SqlParameter> parameters)
    {
      this.parameters = parameters == null ? null : SqlQuery.getParameterList(parameters);
      return this;
    }

    public Builder auth(final AuthenticationResult authResult)
    {
      this.authResult = authResult;
      return this;
    }

    /**
     * Parses the provided {@link #sql} and builds a {@link SqlQueryPlus} with SET statements folded into the
     * context, and with the parsed SQL in {@link #sqlNode}.
     *
     * When using this method, the {@link #sqlNode()} must only be run through validation once. (The validator
     * mutates the {@link SqlNode}).
     */
    public SqlQueryPlus build()
    {
      final StatementAndSetContext statementAndSetContext = DruidSqlParser.parse(sql, true);
      return new SqlQueryPlus(
          sql,
          statementAndSetContext.getMainStatement(),
          true,
          statementAndSetContext.getSetContext().isEmpty()
          ? queryContext
          : QueryContexts.override(queryContext, statementAndSetContext.getSetContext()),
          parameters,
          authResult
      );
    }

    /**
     * Builds a {@link SqlQueryPlus} with no {@link SqlNode} and with {@link #allowSetStatements} set to false.
     * This is done for JDBC becauase it can runs each {@link SqlQueryPlus} multiple times, and it needs to keep
     * re-parsing and re-validating the query on each run.
     *
     * When using this method, you must create a copy with {@link #freshCopy()} prior to calling {@link #sqlNode()}.
     */
    public SqlQueryPlus buildJdbc()
    {
      return new SqlQueryPlus(sql, null, false, queryContext, parameters, authResult);
    }
  }
}
