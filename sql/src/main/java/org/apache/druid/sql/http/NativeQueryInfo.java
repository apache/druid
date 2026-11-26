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
import org.apache.druid.server.RunningQueryRegistry;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Class included in {@link GetQueriesResponse} for queries running on the {@link NativeSqlEngine}. Instances are
 * created from {@link RunningQueryRegistry.Entry}.
 */
public class NativeQueryInfo implements QueryInfo
{
  private final String queryId;
  @Nullable
  private final String sqlQueryId;
  private final String queryType;
  private final List<String> datasources;
  @Nullable
  private final String identity;
  @Nullable
  private final String authenticator;
  @Nullable
  private final String sql;
  @Nullable
  private final String nativeQuery;
  private final DateTime startTime;

  @JsonCreator
  public NativeQueryInfo(
      @JsonProperty("queryId") final String queryId,
      @JsonProperty("sqlQueryId") @Nullable final String sqlQueryId,
      @JsonProperty("queryType") final String queryType,
      @JsonProperty("datasources") final List<String> datasources,
      @JsonProperty("identity") @Nullable final String identity,
      @JsonProperty("authenticator") @Nullable final String authenticator,
      @JsonProperty("sql") @Nullable final String sql,
      @JsonProperty("nativeQuery") @Nullable final String nativeQuery,
      @JsonProperty("startTime") final DateTime startTime
  )
  {
    this.queryId = queryId;
    this.sqlQueryId = sqlQueryId;
    this.queryType = queryType;
    this.datasources = datasources;
    this.identity = identity;
    this.authenticator = authenticator;
    this.sql = sql;
    this.nativeQuery = nativeQuery;
    this.startTime = startTime;
  }

  public static NativeQueryInfo fromEntry(final RunningQueryRegistry.Entry entry)
  {
    return new NativeQueryInfo(
        entry.getQueryId(),
        entry.getSqlQueryId(),
        entry.getQueryType(),
        entry.getDatasources(),
        entry.getIdentity(),
        entry.getAuthenticator(),
        entry.getSql(),
        entry.getNativeQuery(),
        entry.getStartTime()
    );
  }

  /**
   * Returns a copy of this object without the identity and authenticator, for callers that are not authorized to see
   * details about other users' queries.
   */
  public NativeQueryInfo withoutAuthenticationResult()
  {
    return new NativeQueryInfo(queryId, sqlQueryId, queryType, datasources, null, null, sql, nativeQuery, startTime);
  }

  @Override
  public String engine()
  {
    return NativeSqlEngine.NAME;
  }

  @Override
  public String state()
  {
    return StandardQueryState.RUNNING;
  }

  @JsonProperty
  public String getState()
  {
    return state();
  }

  @Override
  public String executionId()
  {
    return queryId;
  }

  /**
   * ID of this native query. This is the ID that can be passed to {@code DELETE /druid/v2/{queryId}} to cancel it.
   */
  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }

  /**
   * The {@code sqlQueryId} of the SQL query that this native query belongs to, if this query was created by the SQL
   * layer. This is the ID that can be passed to {@code DELETE /druid/v2/sql/{sqlQueryId}} to cancel it.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  /**
   * Native query type, such as "timeseries", "topN", or "groupBy".
   */
  @JsonProperty
  public String getQueryType()
  {
    return queryType;
  }

  /**
   * Names of the datasources being queried.
   */
  @JsonProperty
  public List<String> getDatasources()
  {
    return datasources;
  }

  /**
   * Identity of the user that submitted this query.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getIdentity()
  {
    return identity;
  }

  /**
   * Authenticator that authenticated the identity from {@link #getIdentity()}.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getAuthenticator()
  {
    return authenticator;
  }

  /**
   * SQL text of the query, when the query came in through the SQL layer. Only included when the server is configured
   * to capture running query text with {@code druid.server.http.enableRunningQueryText}.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getSql()
  {
    return sql;
  }

  /**
   * JSON-serialized native query. Only included when the server is configured to capture running query text with
   * {@code druid.server.http.enableRunningQueryText}.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getNativeQuery()
  {
    return nativeQuery;
  }

  /**
   * Time at which this query started executing on this server.
   */
  @JsonProperty
  public DateTime getStartTime()
  {
    return startTime;
  }
}
