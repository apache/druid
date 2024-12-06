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

package org.apache.druid.query.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Client representation of {@link org.apache.druid.sql.http.SqlQuery}. This is effectively a lightweight POJO class for
 * use by clients such as {@link org.apache.druid.client.broker.BrokerClient} that doesn't bring in any of the
 * Calcite dependencies and server-side logic from the Broker.
 */
public class ClientSqlQuery
{
  @JsonProperty
  private final String query;

  @JsonProperty
  private final String resultFormat;

  @JsonProperty
  private final boolean header;

  @JsonProperty
  private final boolean typesHeader;

  @JsonProperty
  private final boolean sqlTypesHeader;

  @JsonProperty
  private final Map<String, Object> context;

  @JsonProperty
  private final List<ClientSqlParameter> parameters;

  @JsonCreator
  public ClientSqlQuery(
      @JsonProperty("query") final String query,
      @JsonProperty("resultFormat") final String resultFormat,
      @JsonProperty("header") final boolean header,
      @JsonProperty("typesHeader") final boolean typesHeader,
      @JsonProperty("sqlTypesHeader") final boolean sqlTypesHeader,
      @JsonProperty("context") final Map<String, Object> context,
      @JsonProperty("parameters") final List<ClientSqlParameter> parameters
  )
  {
    this.query = query;
    this.resultFormat = resultFormat;
    this.header = header;
    this.typesHeader = typesHeader;
    this.sqlTypesHeader = sqlTypesHeader;
    this.context = context;
    this.parameters = parameters;
  }

  public String getQuery()
  {
    return query;
  }

  public String getResultFormat()
  {
    return resultFormat;
  }

  public boolean isHeader()
  {
    return header;
  }

  public boolean isTypesHeader()
  {
    return typesHeader;
  }

  public boolean isSqlTypesHeader()
  {
    return sqlTypesHeader;
  }

  public Map<String, Object> getContext()
  {
    return context;
  }

  public List<ClientSqlParameter> getParameters()
  {
    return parameters;
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
    final ClientSqlQuery sqlQuery = (ClientSqlQuery) o;
    return header == sqlQuery.header &&
           typesHeader == sqlQuery.typesHeader &&
           sqlTypesHeader == sqlQuery.sqlTypesHeader &&
           Objects.equals(query, sqlQuery.query) &&
           Objects.equals(resultFormat, sqlQuery.resultFormat) &&
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
    return "ClientSqlQuery{" +
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
