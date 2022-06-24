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

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.tester.QueryRunner.Builder;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines the per-query inputs to the planner: the inputs normally
 * obtained from a SQL query (SQL, context, parameters) along with
 * the authorization result.
 */
public class QueryDefn
{
  private final String sql;
  private final Map<String, Object> context;
  private final List<SqlParameter> parameters;
  private final AuthenticationResult authenticationResult;

  public QueryDefn(
      String sql,
      Map<String, Object> context,
      List<SqlParameter> parameters,
      AuthenticationResult authenticationResult
  )
  {
    super();
    this.sql = sql;
    this.context = context;
    this.parameters = parameters;
    this.authenticationResult = authenticationResult;
  }

  public QueryDefn(
      SqlQuery query,
      AuthenticationResult authenticationResult
  )
  {
    this.sql = query.getQuery();
    this.context = query.getContext();
    this.parameters = query.getParameters();
    this.authenticationResult = authenticationResult;
  }

  public static Builder builder(String sql)
  {
    return new Builder(sql);
  }

  public String sql()
  {
    return sql;
  }

  public Map<String, Object> context()
  {
    return context;
  }

  public List<SqlParameter> parameters()
  {
    return parameters;
  }

  public List<TypedValue> typedParameters()
  {
    return SqlQuery.getParameterList(parameters);
  }

  public AuthenticationResult authResult()
  {
    return authenticationResult;
  }

  public QueryDefn withOverrides(Map<String, Object> overrides)
  {
    if (overrides == null || overrides.isEmpty()) {
      return this;
    }
    Map<String, Object> newContext;
    if (context.isEmpty()) {
      newContext = overrides;
    } else {
      newContext = new HashMap<>();
      newContext.putAll(context);
      newContext.putAll(overrides);
    }
    return new QueryDefn(sql, newContext, parameters, authenticationResult);
  }
}
