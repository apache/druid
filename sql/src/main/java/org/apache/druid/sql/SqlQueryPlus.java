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
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the inputs to a SQL execution request: the statement,the context,
 * parameters, and the authorization result. Pass this around rather than the
 * quad of items. The request can evolve: the context and parameters can be
 * filled in later as needed.
 * <p>
 * SQL requests come from a variety of sources in a variety of formats. Use
 * the {@link Builder} class to create an instance from the information
 * available at each point in the code.
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
  private final Map<String, Object> queryContext;
  private final List<TypedValue> parameters;
  private final AuthenticationResult authResult;

  public SqlQueryPlus(
      String sql,
      Map<String, Object> queryContext,
      List<TypedValue> parameters,
      AuthenticationResult authResult
  )
  {
    this.sql = Preconditions.checkNotNull(sql);
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

  public static Builder builder(SqlQuery sqlQuery)
  {
    return new Builder().query(sqlQuery);
  }

  public String sql()
  {
    return sql;
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
    return new SqlQueryPlus(sql, context, parameters, authResult);
  }

  public SqlQueryPlus withParameters(List<TypedValue> parameters)
  {
    return new SqlQueryPlus(sql, queryContext, parameters, authResult);
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

    public Builder query(SqlQuery sqlQuery)
    {
      this.sql = sqlQuery.getQuery();
      this.queryContext = sqlQuery.getContext();
      this.parameters = sqlQuery.getParameterList();
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

    public SqlQueryPlus build()
    {
      return new SqlQueryPlus(
          sql,
          queryContext,
          parameters,
          authResult
      );
    }
  }
}
