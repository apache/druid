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

package org.apache.druid.msq.exec;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reference to a single run of a particular worker.
 */
public class WorkerRunRef
{
  private static final Logger log = new Logger(WorkerRunRef.class);

  private final AtomicReference<Worker> workerRef = new AtomicReference<>();
  private final AtomicReference<ListenableFuture<?>> futureRef = new AtomicReference<>();

  /**
   * Run the provided worker in the provided executor. Returns a future that resolves when work is complete, or
   * when the worker is canceled.
   */
  public ListenableFuture<?> run(final Worker worker, final ListeningExecutorService exec)
  {
    if (!workerRef.compareAndSet(null, worker)) {
      throw DruidException.defensive("Already run");
    }

    ListenableFuture<?> priorFuture = futureRef.get();
    if (priorFuture != null) {
      return priorFuture;
    }

    final ListenableFuture<?> future = exec.submit(() -> {
      final String originalThreadName = Thread.currentThread().getName();
      try {
        Thread.currentThread().setName(StringUtils.format("%s[%s]", originalThreadName, worker.id()));
        worker.run();
      }
      catch (Throwable t) {
        if (Thread.interrupted() || MSQFaultUtils.isCanceledException(t)) {
          log.debug(t, "Canceled, exiting thread.");
        } else {
          log.warn(t, "Worker[%s] failed and stopped.", worker.id());
        }
      }
      finally {
        Thread.currentThread().setName(originalThreadName);
      }
    });

    if (!futureRef.compareAndSet(null, future)) {
      // Future was set while submitting to the executor. Must have been a concurrent cancel().
      // Interrupt the worker.
      future.cancel(true);
    }

    return futureRef.get();
  }

  public Worker worker()
  {
    final Worker worker = workerRef.get();
    if (worker == null) {
      throw DruidException.defensive("No worker, not run yet?");
    }
    return worker;
  }

  /**
   * Cancel the worker. Interrupts the worker if currently running.
   */
  public void cancel()
  {
    final ListenableFuture<?> existingFuture = futureRef.getAndSet(Futures.immediateFuture(null));
    if (existingFuture != null && !existingFuture.isDone()) {
      existingFuture.cancel(true);
    }
  }

  /**
   * Wait for the worker run to finish. Does not throw exceptions from the future, even if the worker
   * ended exceptionally.
   */
  public void awaitStop()
  {
    final ListenableFuture<?> future = futureRef.get();
    if (future == null) {
      throw DruidException.defensive("Not running");
    }

    try {
      future.get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    catch (ExecutionException | CancellationException ignored) {
      // Do nothing
    }
  }
}
