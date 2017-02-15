/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SqlQuery
{
  private final String query;
  private final Map<String, Object> context;

  @JsonCreator
  public SqlQuery(
      @JsonProperty("query") final String query,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.context = context == null ? ImmutableMap.<String, Object>of() : context;
  }

  @JsonProperty
  public String getQuery()
  {
    return query;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
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

    if (query != null ? !query.equals(sqlQuery.query) : sqlQuery.query != null) {
      return false;
    }
    return context != null ? context.equals(sqlQuery.context) : sqlQuery.context == null;
  }

  @Override
  public int hashCode()
  {
    int result = query != null ? query.hashCode() : 0;
    result = 31 * result + (context != null ? context.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SqlQuery{" +
           "query='" + query + '\'' +
           ", context=" + context +
           '}';
  }
}
