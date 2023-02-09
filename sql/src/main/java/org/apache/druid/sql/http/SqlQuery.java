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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlQuery
{
  public static List<TypedValue> getParameterList(List<SqlParameter> parameters)
  {
    return parameters.stream()
                     // null params are not good!
                     // we pass them to the planner, so that it can generate a proper error message.
                     // see SqlParameterizerShuttle and RelParameterizerShuttle.
                     .map(p -> p == null ? null : p.getTypedValue())
                     .collect(Collectors.toList());
  }

  private final String query;
  private final ResultFormat resultFormat;
  private final boolean header;
  private final boolean typesHeader;
  private final boolean sqlTypesHeader;
  private final Map<String, Object> context;
  private final List<SqlParameter> parameters;

  @JsonCreator
  public SqlQuery(
      @JsonProperty("query") final String query,
      @JsonProperty("resultFormat") final ResultFormat resultFormat,
      @JsonProperty("header") final boolean header,
      @JsonProperty("typesHeader") final boolean typesHeader,
      @JsonProperty("sqlTypesHeader") final boolean sqlTypesHeader,
      @JsonProperty("context") final Map<String, Object> context,
      @JsonProperty("parameters") final List<SqlParameter> parameters
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.resultFormat = resultFormat == null ? ResultFormat.OBJECT : resultFormat;
    this.header = header;
    this.typesHeader = typesHeader;
    this.sqlTypesHeader = sqlTypesHeader;
    this.context = context == null ? ImmutableMap.of() : context;
    this.parameters = parameters == null ? ImmutableList.of() : parameters;

    if (typesHeader && !header) {
      throw new ISE("Cannot include 'typesHeader' without 'header'");
    }

    if (sqlTypesHeader && !header) {
      throw new ISE("Cannot include 'sqlTypesHeader' without 'header'");
    }
  }

  public SqlQuery withOverridenContext(Map<String, Object> overridenContext)
  {
    return new SqlQuery(
        getQuery(),
        getResultFormat(),
        includeHeader(),
        includeTypesHeader(),
        includeSqlTypesHeader(),
        overridenContext,
        getParameters()
    );
  }

  @JsonProperty
  public String getQuery()
  {
    return query;
  }

  @JsonProperty
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty("header")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean includeHeader()
  {
    return header;
  }

  @JsonProperty("typesHeader")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean includeTypesHeader()
  {
    return typesHeader;
  }

  @JsonProperty("sqlTypesHeader")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean includeSqlTypesHeader()
  {
    return sqlTypesHeader;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  public QueryContext queryContext()
  {
    return QueryContext.of(context);
  }

  @JsonProperty
  public List<SqlParameter> getParameters()
  {
    return parameters;
  }

  public List<TypedValue> getParameterList()
  {
    return getParameterList(parameters);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlQuery sqlQuery = (SqlQuery) o;
    return header == sqlQuery.header &&
           typesHeader == sqlQuery.typesHeader &&
           sqlTypesHeader == sqlQuery.sqlTypesHeader &&
           Objects.equals(query, sqlQuery.query) &&
           resultFormat == sqlQuery.resultFormat &&
           Objects.equals(context, sqlQuery.context) &&
           Objects.equals(parameters, sqlQuery.parameters);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, resultFormat, header, typesHeader, sqlTypesHeader, context, parameters);
  }

  @Override
  public String toString()
  {
    return "SqlQuery{" +
           "query='" + query + '\'' +
           ", resultFormat=" + resultFormat +
           ", header=" + header +
           ", typesHeader=" + typesHeader +
           ", sqlTypesHeader=" + sqlTypesHeader +
           ", context=" + context +
           ", parameters=" + parameters +
           '}';
  }
}
