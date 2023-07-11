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

import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceRetryPolicy;

/**
 * Utility function for creating {@link ServiceClient} instances that communicate to indexing service tasks.
 */
public class TaskServiceClients
{
  private TaskServiceClients()
  {
    // No instantiation.
  }

  /**
   * Makes a {@link ServiceClient} linked to the provided task. The client's base path comes pre-set to the
   * chat handler resource of the task: {@code /druid/worker/v1/chat/<taskId>}.
   */
  public static ServiceClient makeClient(
      final String taskId,
      final ServiceRetryPolicy baseRetryPolicy,
      final ServiceClientFactory serviceClientFactory,
      final OverlordClient overlordClient
  )
  {
    // SpecificTaskServiceLocator is Closeable, but the close() method does not actually free resources, so there
    // is no need to ensure it is ever closed. Relying on GC is OK.
    final SpecificTaskServiceLocator serviceLocator = new SpecificTaskServiceLocator(taskId, overlordClient);
    final SpecificTaskRetryPolicy retryPolicy = new SpecificTaskRetryPolicy(taskId, baseRetryPolicy);
    return serviceClientFactory.makeClient(taskId, serviceLocator, retryPolicy);
  }
}
