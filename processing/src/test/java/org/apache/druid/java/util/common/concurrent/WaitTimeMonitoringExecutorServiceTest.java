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

package org.apache.druid.java.util.common.concurrent;

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class WaitTimeMonitoringExecutorServiceTest
{

  @Test
  public void testVerifyEmittedMetrics() throws InterruptedException
  {
    StubServiceEmitter stubServiceEmitter = new StubServiceEmitter("service", "host");
    WaitTimeMonitoringExecutorService executorService = new WaitTimeMonitoringExecutorService(
        Execs.newBlockingThreaded("poolPrefix-%d", 1, 1),
        stubServiceEmitter,
        "metricPrefix"
    );

    executorService.submit(() -> {
      System.out.println("TEST!");
    });
    executorService.submit(() -> {
      System.out.println("TEST Runnable with Result!");
    }, "result");
    executorService.submit(() -> {
      System.out.println("TEST Callable!");
      return null;
    });

    stubServiceEmitter.verifyEmitted("metricPrefix/taskQueuedDuration", 3);
    stubServiceEmitter.verifyEmitted("metricPrefix/queuedTasks", 3);
    stubServiceEmitter.verifyEmitted("metricPrefix/taskCount", 3);
    stubServiceEmitter.verifyEmitted("metricPrefix/activeThreads", 3);
    stubServiceEmitter.verifyEmitted("metricPrefix/completedTasks", 3);

    stubServiceEmitter.verifyNotEmitted("someRandomMetricName");

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);
    Assert.assertTrue(executorService.isTerminated());
  }
}
