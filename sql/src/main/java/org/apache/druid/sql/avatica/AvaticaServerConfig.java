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

package org.apache.druid.sql.avatica;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Period;

class AvaticaServerConfig
{
  public static int DEFAULT_MAX_CONNECTIONS = 25;
  public static int DEFAULT_MAX_STATEMENTS_PER_CONNECTION = 4;
  public static Period DEFAULT_CONNECTION_IDLE_TIMEOUT = new Period("PT5M");
  public static int DEFAULT_MIN_ROWS_PER_FRAME = 100;
  public static int DEFAULT_MAX_ROWS_PER_FRAME = 5000;
  public static int DEFAULT_FETCH_TIMEOUT_MS = 5000;

  @JsonProperty
  public int maxConnections = DEFAULT_MAX_CONNECTIONS;

  @JsonProperty
  public int maxStatementsPerConnection = DEFAULT_MAX_STATEMENTS_PER_CONNECTION;

  @JsonProperty
  public Period connectionIdleTimeout = DEFAULT_CONNECTION_IDLE_TIMEOUT;

  @JsonProperty
  public int minRowsPerFrame = DEFAULT_MIN_ROWS_PER_FRAME;

  @JsonProperty
  public int maxRowsPerFrame = DEFAULT_MAX_ROWS_PER_FRAME;

  /**
   * The maximum amount of time to wait per-fetch for the next result set.
   * If a query takes longer than this amount of time, then the fetch will
   * return 0 rows, without EOF, and the client will automatically try
   * another fetch. The result is an async protocol that avoids network
   * timeouts for long-running queries, especially those that take a long
   * time to deliver a first batch of results.
   */
  @JsonProperty
  public int fetchTimeoutMs = DEFAULT_FETCH_TIMEOUT_MS;

  public int getMaxConnections()
  {
    return maxConnections;
  }

  public int getMaxStatementsPerConnection()
  {
    return maxStatementsPerConnection;
  }

  public Period getConnectionIdleTimeout()
  {
    return connectionIdleTimeout;
  }

  public int getMaxRowsPerFrame()
  {
    return maxRowsPerFrame;
  }

  public int getMinRowsPerFrame()
  {
    Preconditions.checkArgument(
        minRowsPerFrame > 0,
        "'druid.sql.avatica.minRowsPerFrame' must be set to a value greater than 0"
    );
    if (maxRowsPerFrame > 0) {
      return Math.min(getMaxRowsPerFrame(), minRowsPerFrame);
    }
    return minRowsPerFrame;
  }

  public int getFetchTimeoutMs()
  {
    return fetchTimeoutMs;
  }
}
