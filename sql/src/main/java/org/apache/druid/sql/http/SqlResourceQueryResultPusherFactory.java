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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.sql.HttpStatement;

import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;

public class SqlResourceQueryResultPusherFactory
{
  private final ObjectMapper jsonMapper;
  private final ServerConfig serverConfig;
  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;

  @Inject
  public SqlResourceQueryResultPusherFactory(
      ObjectMapper jsonMapper,
      ServerConfig serverConfig,
      ResponseContextConfig responseContextConfig,
      @Self DruidNode selfNode
  )
  {
    this.jsonMapper = jsonMapper;
    this.serverConfig = serverConfig;
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
  }

  public SqlResourceQueryResultPusher factorize(
      final QueryCountStatsProvider counter,
      HttpServletRequest req,
      HttpStatement stmt,
      SqlQuery sqlQuery
  )
  {
    Map<String, String> headers = new LinkedHashMap<>();

    final String sqlQueryId = stmt.sqlQueryId();
    headers.put(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId);

    if (sqlQuery.includeHeader()) {
      headers.put(SqlResource.SQL_HEADER_RESPONSE_HEADER, SqlResource.SQL_HEADER_VALUE);
    }

    return new SqlResourceQueryResultPusher(
        jsonMapper,
        responseContextConfig,
        selfNode,
        serverConfig,
        req,
        stmt,
        sqlQuery,
        headers,
        counter
    );
  }
}
