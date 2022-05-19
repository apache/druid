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

package org.apache.druid.query;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This interface is in a very early stage and should not be considered stable.
 *
 * The purpose of the QueryWatcher is to give overall visibility into queries running
 * or pending at the QueryRunner level. This is currently used to cancel all the
 * parts of a pending query, but may be expanded in the future to offer more direct
 * visibility into query execution and resource usage.
 *
 * QueryRunners executing any computation asynchronously must register their queries
 * with the QueryWatcher.
 *
 */
public interface QueryWatcher
{
  /**
   * QueryRunners must use this method to register any pending queries.
   *
   * The given future may have cancel(true) called at any time, if cancellation of this query has been requested.
   *
   * @param query a query, which may be a subset of a larger query, as long as the underlying queryId is unchanged
   * @param future the future holding the execution status of the query
   */
  void registerQueryFuture(Query<?> query, ListenableFuture<?> future);
}
