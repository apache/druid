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

package org.apache.druid.client.indexing;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;

import java.util.Map;
import java.util.Set;

public class NoopOverlordClient implements OverlordClient
{
  @Override
  public ListenableFuture<Void> runTask(String taskId, Object taskObject)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Map<String, Object>> taskReportAsMap(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public OverlordClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    // Ignore retryPolicy for the test client.
    return this;
  }
}
