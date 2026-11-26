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

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.server.security.AuthenticationResult;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private final Map<String, Entry> entries = new ConcurrentHashMap<>();

  /**
   * Registers a query that is about to start executing.
   */
  public void register(final Query<?> query, @Nullable final AuthenticationResult authenticationResult)
  {
    final String queryId = query.getId();
    if (queryId == null) {
      return;
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
    retVal.sort((a, b) -> {
      final int timeCompare = a.getStartTime().compareTo(b.getStartTime());
      return timeCompare != 0 ? timeCompare : a.getQueryId().compareTo(b.getQueryId());
    });
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
    private final DateTime startTime;

    public Entry(
        final String queryId,
        @Nullable final String sqlQueryId,
        final String queryType,
        final List<String> datasources,
        @Nullable final String identity,
        @Nullable final String authenticator,
        final DateTime startTime
    )
    {
      this.queryId = queryId;
      this.sqlQueryId = sqlQueryId;
      this.queryType = queryType;
      this.datasources = datasources;
      this.identity = identity;
      this.authenticator = authenticator;
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

    public DateTime getStartTime()
    {
      return startTime;
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
      final Entry entry = (Entry) o;
      return Objects.equals(queryId, entry.queryId)
             && Objects.equals(sqlQueryId, entry.sqlQueryId)
             && Objects.equals(queryType, entry.queryType)
             && Objects.equals(datasources, entry.datasources)
             && Objects.equals(identity, entry.identity)
             && Objects.equals(authenticator, entry.authenticator)
             && Objects.equals(startTime, entry.startTime);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(queryId, sqlQueryId, queryType, datasources, identity, authenticator, startTime);
    }

    @Override
    public String toString()
    {
      return "Entry{" +
             "queryId='" + queryId + '\'' +
             ", sqlQueryId='" + sqlQueryId + '\'' +
             ", queryType='" + queryType + '\'' +
             ", datasources=" + datasources +
             ", identity='" + identity + '\'' +
             ", authenticator='" + authenticator + '\'' +
             ", startTime=" + startTime +
             '}';
    }
  }
}
