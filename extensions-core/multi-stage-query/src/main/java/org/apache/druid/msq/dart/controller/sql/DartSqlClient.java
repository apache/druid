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

package org.apache.druid.msq.dart.controller.sql;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.msq.dart.controller.http.DartSqlResource;
import org.apache.druid.msq.dart.controller.http.GetQueriesResponse;

import javax.servlet.http.HttpServletRequest;

/**
 * Client for the {@link DartSqlResource} resource.
 */
public interface DartSqlClient
{
  /**
   * Get information about all currently-running queries on this server.
   *
   * @param selfOnly true if only queries from this server should be returned; false if queries from all servers
   *                 should be returned
   *
   * @see DartSqlResource#doGetRunningQueries(String, HttpServletRequest) the server side
   */
  ListenableFuture<GetQueriesResponse> getRunningQueries(boolean selfOnly);
}
