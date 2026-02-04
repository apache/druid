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

package org.apache.druid.k8s.overlord.common.httpclient.vertx;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the Vertx HTTP client used by Kubernetes client.
 *
 * These settings control the thread pool sizes for the Vertx event loop.
 * Tune these based on your expected concurrent K8s API call volume.
 *
 * Backported from Druid 35 PR #18540.
 */
public class DruidKubernetesVertxHttpClientConfig
{
  // Defaults match VertxOptions.DEFAULT_WORKER_POOL_SIZE
  public static final int DEFAULT_WORKER_POOL_SIZE = 20;

  // Default event loop = 2 * number of CPU cores (0 means use Vertx default)
  public static final int DEFAULT_EVENT_LOOP_POOL_SIZE = 0;

  // Defaults match VertxOptions.DEFAULT_INTERNAL_BLOCKING_POOL_SIZE
  public static final int DEFAULT_INTERNAL_BLOCKING_POOL_SIZE = 20;

  @JsonProperty
  private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;

  @JsonProperty
  private int eventLoopPoolSize = DEFAULT_EVENT_LOOP_POOL_SIZE;

  @JsonProperty
  private int internalBlockingPoolSize = DEFAULT_INTERNAL_BLOCKING_POOL_SIZE;

  public int getWorkerPoolSize()
  {
    return workerPoolSize;
  }

  public int getEventLoopPoolSize()
  {
    return eventLoopPoolSize;
  }

  public int getInternalBlockingPoolSize()
  {
    return internalBlockingPoolSize;
  }

  @Override
  public String toString()
  {
    return "DruidKubernetesVertxHttpClientConfig{" +
           "workerPoolSize=" + workerPoolSize +
           ", eventLoopPoolSize=" + eventLoopPoolSize +
           ", internalBlockingPoolSize=" + internalBlockingPoolSize +
           '}';
  }
}
