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

package org.apache.druid.client.broker;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.query.explain.ExplainPlan;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;

import java.util.List;

/**
 * High-level Broker client.
 * <p>
 * All methods return futures, enabling asynchronous logic. If you want a synchronous response, use
 * {@code FutureUtils.get} or {@code FutureUtils.getUnchecked}.
 * Futures resolve to exceptions in the manner described by {@link org.apache.druid.rpc.ServiceClient#asyncRequest}.
 * </p>
 * Typically acquired via Guice, where it is registered using {@link org.apache.druid.rpc.guice.ServiceClientModule}.
 */
public interface BrokerClient
{
  /**
   * Submit the given {@code sqlQuery} to the Broker's SQL task endpoint.
   */
  ListenableFuture<SqlTaskStatus> submitSqlTask(ClientSqlQuery sqlQuery);

  /**
   * Fetches the explain plan for the given {@code sqlQuery} from the Broker's SQL task endpoint.
   *
   * @param sqlQuery the SQL query for which the {@code EXPLAIN PLAN FOR} information is to be fetched
   */
  ListenableFuture<List<ExplainPlan>> fetchExplainPlan(ClientSqlQuery sqlQuery);
}
