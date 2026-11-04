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

package org.apache.druid.jdbc.http;

import java.sql.SQLException;

/**
 * HTTP client for Druid SQL.
 */
public interface DruidHttpClient extends AutoCloseable
{
  /**
   * Run a SQL query.
   *
   * @throws SQLException if the query execution fails prior to returning any results
   */
  QueryResultsIterator runQuery(SqlRequest request) throws SQLException;

  /**
   * Cancels a running SQL query.
   *
   * @throws SQLException if the cancellation request fails
   */
  void cancelQuery(String sqlQueryId) throws SQLException;

  /**
   * Returns the URL for this client, like {@code https://example.com:9088/druid/v2/sql/}.
   */
  String getUrl();

  /**
   * Returns the per-request network timeout in milliseconds, or zero if none is set.
   */
  int getNetworkTimeoutMillis();

  /**
   * Sets the per-request network timeout, in milliseconds, applied to HTTP requests started after this call.
   * Zero means no client-side request timeout.
   */
  void setNetworkTimeoutMillis(int networkTimeoutMillis);

  /**
   * Returns whether the client is closed.
   */
  boolean isClosed();

  /**
   * Closes the HTTP client and releases resources. Aborts any in-flight requests.
   */
  @Override
  void close();
}
