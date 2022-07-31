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
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Captures the inputs to a SQL execution request: the statement,
 * the context, parameters, and the authorization result. Pass this
 * around rather than the quad of items. The request can evolve:
 * items can be filled in later as needed (except for the SQL
 * and auth result, which is required.)
 */
public class SqlQueryPlus
{
  private final String sql;
  private final QueryContext queryContext;
  private final List<TypedValue> parameters;
  private final AuthenticationResult authResult;

  public SqlQueryPlus(
      String sql,
      QueryContext queryContext,
      List<TypedValue> parameters,
      AuthenticationResult authResult
  )
  {
    this.sql = Preconditions.checkNotNull(sql);
    this.queryContext = queryContext == null
        ? new QueryContext()
        : queryContext;
    this.parameters = parameters == null
        ? Collections.emptyList()
        : parameters;
    this.authResult = Preconditions.checkNotNull(authResult);
  }

  public SqlQueryPlus(final String sql, final AuthenticationResult authResult)
  {
    this(sql, (QueryContext) null, null, authResult);
  }

  public SqlQueryPlus(
      String sql,
      QueryContext queryContext,
      AuthenticationResult authResult
  )
  {
    this(sql, queryContext, null, authResult);
  }

  public static SqlQueryPlus fromQuery(SqlQuery sqlQuery, final AuthenticationResult authResult)
  {
    return new SqlQueryPlus(
        sqlQuery.getQuery(),
        new QueryContext(sqlQuery.getContext()),
        sqlQuery.getParameterList(),
        authResult
    );
  }

  public static SqlQueryPlus fromSqlParameters(
      String sql,
      Map<String, Object> queryContext,
      List<SqlParameter> parameters,
      AuthenticationResult authResult
  )
  {
    return new SqlQueryPlus(
        sql,
        queryContext == null ? null : new QueryContext(queryContext),
        parameters == null ? null : SqlQuery.getParameterList(parameters),
        authResult
     );
  }

  public static SqlQueryPlus from(
      String sql,
      Map<String, Object> queryContext,
      List<TypedValue> parameters,
      AuthenticationResult authResult
  )
  {
    return new SqlQueryPlus(
        sql,
        queryContext == null ? null : new QueryContext(queryContext),
        parameters,
        authResult
    );
  }

  public String sql()
  {
    return sql;
  }

  public QueryContext context()
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

  public SqlQueryPlus withContext(QueryContext context)
  {
    return new SqlQueryPlus(sql, context, parameters, authResult);
  }

  public SqlQueryPlus withContext(Map<String, Object> context)
  {
    return new SqlQueryPlus(sql, new QueryContext(context), parameters, authResult);
  }

  public SqlQueryPlus withParameters(List<TypedValue> parameters)
  {
    return new SqlQueryPlus(sql, queryContext, parameters, authResult);
  }

  public SqlQueryPlus withOverrides(Map<String, Object> overrides)
  {
    return new SqlQueryPlus(
        sql,
        queryContext.withOverrides(overrides),
        parameters,
        authResult);
  }
}
