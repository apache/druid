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

package org.apache.druid.guice;


import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class LifecycleForkJoinPoolProvider
{
  private static final Logger LOG = new Logger(LifecycleForkJoinPoolProvider.class);
  private final long awaitShutdownMillis;
  private final ForkJoinPool pool;

  public LifecycleForkJoinPoolProvider(
      int parallelism,
      ForkJoinPool.ForkJoinWorkerThreadFactory factory,
      Thread.UncaughtExceptionHandler handler,
      boolean asyncMode,
      long awaitShutdownMillis
  )
  {
    this.pool = new ForkJoinPool(parallelism, factory, handler, asyncMode);
    this.awaitShutdownMillis = awaitShutdownMillis;
  }

  @LifecycleStop
  public void stop()
  {
    LOG.info("Shutting down ForkJoinPool [%s]", this);
    pool.shutdown();
    try {
      if (!pool.awaitTermination(awaitShutdownMillis, TimeUnit.MILLISECONDS)) {
        LOG.warn("Failed to complete all tasks in FJP [%s]", this);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted on shutdown", e);
    }
  }

  public ForkJoinPool getPool()
  {
    return pool;
  }
}
