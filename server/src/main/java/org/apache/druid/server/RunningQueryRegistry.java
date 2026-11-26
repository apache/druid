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

package org.apache.druid.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of the queries that are currently executing on this process.
 * <p>
 * Queries are added by {@link QueryLifecycle} when execution starts, and removed when execution finishes. The registry
 * is a best-effort, in-memory view of in-flight queries: it is local to this process and is not persisted. It is used
 * by the "list running queries" APIs, such as {@code GET /druid/v2/sql/queries} and {@code sys.queries}.
 */
@LazySingleton
public class RunningQueryRegistry
{
  private final ServerConfig serverConfig;
  private final Map<String, Entry> entries = new ConcurrentHashMap<>();

  @Inject
  public RunningQueryRegistry(final ServerConfig serverConfig)
  {
    this.serverConfig = serverConfig;
  }

  @VisibleForTesting
  public RunningQueryRegistry()
  {
    this(new ServerConfig());
  }

  /**
   * Registers a query that is about to start executing.
   *
   * @param query                query that is starting
   * @param sqlText              SQL text of the query, when the query came in through the SQL layer
   * @param authenticationResult authentication result for the user that submitted the query
   */
  public void register(
      final Query<?> query,
      @Nullable final String sqlText,
      @Nullable final AuthenticationResult authenticationResult
  )
  {
    final String queryId = query.getId();
    if (queryId == null) {
      return;
    }

    String capturedSqlText = null;
    Query<?> capturedNativeQuery = null;

    if (serverConfig.isEnableRunningQueryText()) {
      capturedSqlText = sqlText;
      capturedNativeQuery = query;
    }

    entries.put(
        queryId,
        new Entry(
            queryId,
            query.getSqlQueryId(),
            query.getType(),
            List.copyOf(query.getDataSource().getTableNames()),
            authenticationResult == null ? null : authenticationResult.getIdentity(),
            authenticationResult == null ? null : authenticationResult.getAuthenticatedBy(),
            capturedSqlText,
            capturedNativeQuery,
            DateTimes.nowUtc()
        )
    );
  }

  /**
   * Removes a query that has finished executing. Does nothing if the query was never registered.
   */
  public void unregister(final String queryId)
  {
    if (queryId != null) {
      entries.remove(queryId);
    }
  }

  /**
   * Returns a snapshot of all currently executing queries, sorted by start time.
   */
  public List<Entry> getAll()
  {
    final List<Entry> retVal = new ArrayList<>(entries.values());
    retVal.sort(Comparator.comparing(Entry::getStartTime).thenComparing(Entry::getQueryId));
    return retVal;
  }

  /**
   * Information about a single running query.
   */
  public static class Entry
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
    private final Query<?> nativeQuery;
    private final DateTime startTime;

    public Entry(
        final String queryId,
        @Nullable final String sqlQueryId,
        final String queryType,
        final List<String> datasources,
        @Nullable final String identity,
        @Nullable final String authenticator,
        @Nullable final String sql,
        @Nullable final Query<?> nativeQuery,
        final DateTime startTime
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

    public String getQueryId()
    {
      return queryId;
    }

    /**
     * The {@code sqlQueryId} of the SQL query that this native query belongs to, or null for native queries that were
     * not created by the SQL layer.
     */
    @Nullable
    public String getSqlQueryId()
    {
      return sqlQueryId;
    }

    public String getQueryType()
    {
      return queryType;
    }

    public List<String> getDatasources()
    {
      return datasources;
    }

    @Nullable
    public String getIdentity()
    {
      return identity;
    }

    @Nullable
    public String getAuthenticator()
    {
      return authenticator;
    }

    /**
     * SQL text of this query, when the query came in through the SQL layer. Null unless
     * {@link ServerConfig#isEnableRunningQueryText()} is enabled.
     */
    @Nullable
    public String getSql()
    {
      return sql;
    }

    /**
     * Native query that is running. Null unless {@link ServerConfig#isEnableRunningQueryText()} is enabled.
     */
    @Nullable
    public Query<?> getNativeQuery()
    {
      return nativeQuery;
    }

    public DateTime getStartTime()
    {
      return startTime;
    }
  }
}
