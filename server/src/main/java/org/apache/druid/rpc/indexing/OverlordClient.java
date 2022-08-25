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

package org.apache.druid.rpc.indexing;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.rpc.ServiceRetryPolicy;

import java.util.Map;
import java.util.Set;

/**
 * High-level Overlord client.
 *
 * Similar to {@link org.apache.druid.client.indexing.IndexingServiceClient}, but backed by
 * {@link org.apache.druid.rpc.ServiceClient} instead of {@link org.apache.druid.discovery.DruidLeaderClient}.
 *
 * All methods return futures, enabling asynchronous logic. If you want a synchronous response, use
 * {@code FutureUtils.get} or {@code FutureUtils.getUnchecked}.
 *
 * Futures resolve to exceptions in the manner described by {@link org.apache.druid.rpc.ServiceClient#asyncRequest}.
 *
 * Typically acquired via Guice, where it is registered using {@link org.apache.druid.rpc.guice.ServiceClientModule}.
 */
public interface OverlordClient
{
  ListenableFuture<Void> runTask(String taskId, Object taskObject);

  ListenableFuture<Void> cancelTask(String taskId);

  ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds);

  ListenableFuture<TaskStatusResponse> taskStatus(String taskId);

  ListenableFuture<Map<String, Object>> taskReportAsMap(String taskId);

  OverlordClient withRetryPolicy(ServiceRetryPolicy retryPolicy);
}
