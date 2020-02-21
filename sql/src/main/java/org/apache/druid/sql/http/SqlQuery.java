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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.remote.TypedValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlQuery
{
  public static List<TypedValue> getParameterList(List<SqlParameter> parameters)
  {
    return parameters.stream()
                     .map(SqlParameter::getTypedValue)
                     .collect(Collectors.toList());
  }

  private final String query;
  private final ResultFormat resultFormat;
  private final boolean header;
  private final Map<String, Object> context;
  private final List<SqlParameter> parameters;

  @JsonCreator
  public SqlQuery(
      @JsonProperty("query") final String query,
      @JsonProperty("resultFormat") final ResultFormat resultFormat,
      @JsonProperty("header") final boolean header,
      @JsonProperty("context") final Map<String, Object> context,
      @JsonProperty("parameters") final List<SqlParameter> parameters
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.resultFormat = resultFormat == null ? ResultFormat.OBJECT : resultFormat;
    this.header = header;
    this.context = context == null ? ImmutableMap.of() : context;
    this.parameters = parameters == null ? ImmutableList.of() : parameters;
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
  public boolean includeHeader()
  {
    return header;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
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
           Objects.equals(query, sqlQuery.query) &&
           resultFormat == sqlQuery.resultFormat &&
           Objects.equals(context, sqlQuery.context) &&
           Objects.equals(parameters, sqlQuery.parameters);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, resultFormat, header, context, parameters);
  }

  @Override
  public String toString()
  {
    return "SqlQuery{" +
           "query='" + query + '\'' +
           ", resultFormat=" + resultFormat +
           ", header=" + header +
           ", context=" + context +
           ", parameters=" + parameters +
           '}';
  }
}
