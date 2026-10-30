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

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.security.AuthenticationResult;

/**
 * Creates a runner for a datasource whose execution is not provided by the normal segment walker. The query may
 * contain the registered datasource below its root; the handler is responsible for resolving every matching vertex
 * before delegating the remaining query to normal native execution.
 */
public interface DataSourceQueryHandler
{
  /**
   * Creates the datasource runner for an authenticated request.
   *
   * @param executeLocally whether the request selected local execution with
   *                       {@link QueryResource#HEADER_NATIVE_QUERY_ROUTE}
   */
  <T> QueryRunner<T> createRunner(
      Query<T> query,
      AuthenticationResult authenticationResult,
      boolean executeLocally
  );
}
