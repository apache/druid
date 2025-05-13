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

package org.apache.druid.msq.dart.controller;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.QueryInfo;
import org.apache.druid.sql.http.QueryManager;
import org.apache.druid.sql.http.SqlResource;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class DartQueryManager implements QueryManager
{
  private static final Logger log = new Logger(SqlResource.class);
  private final DartControllerRegistry controllerRegistry;
  private final DartSqlClients sqlClients;

  @Inject
  public DartQueryManager(DartControllerRegistry controllerRegistry, DartSqlClients sqlClients)
  {
    log.error("CREATED");
    this.controllerRegistry = controllerRegistry;
    this.sqlClients = sqlClients;
  }

  @Override
  public List<QueryInfo> getRunningQueries(boolean selfOnly)
  {
    final List<DartQueryInfo> queries = controllerRegistry.getAllHolders()
                                                          .stream()
                                                          .map(DartQueryInfo::fromControllerHolder)
                                                          .collect(Collectors.toList());

    // Add queries from all other servers, if "selfOnly" is false.
    if (!selfOnly) {
      final List<GetQueriesResponse> otherQueries = FutureUtils.getUnchecked(
          Futures.successfulAsList(
              Iterables.transform(sqlClients.getAllClients(), client -> client.getRunningQueries(true))),
          true
      );

      for (final GetQueriesResponse response : otherQueries) {
        if (response != null) {
          response.getQueries().stream()
                  .filter(queryInfo -> (queryInfo instanceof DartQueryInfo))
                  .map(queryInfo -> (DartQueryInfo) queryInfo)
                  .forEach(queries::add);
        }
      }
    }

    // Sort queries by start time, breaking ties by query ID, so the list comes back in a consistent and nice order.
    queries.sort(Comparator.comparing(DartQueryInfo::getStartTime).thenComparing(DartQueryInfo::getDartQueryId));
    return List.copyOf(queries);
  }
}
