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

package org.apache.druid.testing.cluster.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClientImpl;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FaultyOverlordClient extends OverlordClientImpl
{
  private static final Logger log = new Logger(FaultyOverlordClient.class);

  private final ObjectMapper jsonMapper;
  private final ServiceClient serviceClient;
  private final ClusterTestingTaskConfig.OverlordClientConfig testingConfig;
  private final CountDownLatch taskStatusDelayEntered;
  private final CountDownLatch taskStatusDelayReleased;

  @Inject
  public FaultyOverlordClient(
      ClusterTestingTaskConfig.OverlordClientConfig testingConfig,
      @Json final ObjectMapper jsonMapper,
      @IndexingService final ServiceClient serviceClient
  )
  {
    this(
        testingConfig,
        jsonMapper,
        serviceClient,
        new CountDownLatch(1),
        new CountDownLatch(1)
    );
  }

  private FaultyOverlordClient(
      ClusterTestingTaskConfig.OverlordClientConfig testingConfig,
      ObjectMapper jsonMapper,
      ServiceClient serviceClient,
      CountDownLatch taskStatusDelayEntered,
      CountDownLatch taskStatusDelayReleased
  )
  {
    super(serviceClient, jsonMapper);
    this.jsonMapper = jsonMapper;
    this.serviceClient = serviceClient;
    this.testingConfig = testingConfig;
    this.taskStatusDelayEntered = taskStatusDelayEntered;
    this.taskStatusDelayReleased = taskStatusDelayReleased;
    log.info("Initialized FaultyOverlordClient with config[%s]", testingConfig);
  }

  @Override
  public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
      @Nullable String state,
      @Nullable String dataSource,
      @Nullable Integer maxCompletedTasks
  )
  {
    addDelayIfConfigured();
    return super.taskStatuses(state, dataSource, maxCompletedTasks);
  }

  @Override
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
  {
    addDelayIfConfigured();
    return super.taskStatuses(taskIds);
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    addDelayIfConfigured();
    return super.taskStatus(taskId);
  }

  @Override
  public OverlordClientImpl withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new FaultyOverlordClient(
        testingConfig,
        jsonMapper,
        serviceClient,
        taskStatusDelayEntered,
        taskStatusDelayReleased
    );
  }

  public boolean awaitTaskStatusDelayEntered(long timeout, TimeUnit unit) throws InterruptedException
  {
    return taskStatusDelayEntered.await(timeout, unit);
  }

  public void releaseTaskStatusDelay()
  {
    taskStatusDelayReleased.countDown();
  }

  private void addDelayIfConfigured()
  {
    final Duration delay = testingConfig.getTaskStatusDelay();
    if (delay == null) {
      return;
    }

    taskStatusDelayEntered.countDown();
    try {
      log.info("Waiting for [%s] before calling Overlord", delay);
      taskStatusDelayReleased.await(delay.getMillis(), TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.info("Interrupted while waiting before task action.");
    }
  }
}
