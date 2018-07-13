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

package io.druid.guice;

import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class LifecycleForkJoinPool extends ForkJoinPool
{
  private static final Logger LOG = new Logger(LifecycleForkJoinPool.class);

  public LifecycleForkJoinPool(
      int parallelism,
      ForkJoinWorkerThreadFactory factory,
      Thread.UncaughtExceptionHandler handler,
      boolean asyncMode
  )
  {
    super(parallelism, factory, handler, asyncMode);
  }

  @LifecycleStop
  public void stop()
  {
    LOG.info("Shutting down ForkJoinPool [%s]", this);
    shutdown();
    try {
      // Should this be configurable?
      if (!awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("Failed to complete all tasks in FJP [%s]", this);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted on shutdown", e);
    }
  }
}
