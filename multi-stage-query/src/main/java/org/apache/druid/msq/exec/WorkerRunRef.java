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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Reference to a single run of a particular worker.
 */
public class WorkerRunRef
{
  private static final Logger log = new Logger(WorkerRunRef.class);

  @GuardedBy("this")
  private Worker worker;

  @GuardedBy("this")
  private ListenableFuture<?> workerRunFuture;

  @GuardedBy("this")
  private boolean canceled;

  /**
   * Run the provided worker in the provided executor. Returns a future that resolves when work is complete, or
   * when the worker is canceled.
   */
  public synchronized ListenableFuture<?> run(final Worker worker, final ListeningExecutorService exec)
  {
    if (this.worker != null) {
      throw DruidException.defensive("Already run");
    }

    if (canceled) {
      // cancel() called before run()
      return Futures.immediateFailedFuture(new CancellationException());
    }

    this.worker = worker;

    return workerRunFuture = exec.submit(() -> {
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
  }

  public synchronized Worker worker()
  {
    if (worker == null) {
      throw DruidException.defensive("No worker, not run yet?");
    }
    return worker;
  }

  /**
   * Returns true if the worker reference has been set (i.e., {@link #run} has been called).
   */
  public synchronized boolean hasWorker()
  {
    return worker != null;
  }

  /**
   * Cancel the worker. Interrupts the worker if currently running.
   */
  public synchronized void cancel()
  {
    if (worker == null) {
      canceled = true;
      return;
    }

    // Interrupt the worker's run future, so the run() thread stops executing.
    if (workerRunFuture != null) {
      workerRunFuture.cancel(true);
    }

    // Also directly signal the run() thread to stop. Ideally this shouldn't be necessary, since the
    // interrupt should be enough.
    worker.stop();
  }

  /**
   * Wait for the worker run to finish. Does not throw exceptions from the future, even if the worker
   * ended exceptionally.
   */
  public synchronized void awaitStop()
  {
    if (workerRunFuture == null) {
      throw DruidException.defensive("Not running");
    }

    try {
      workerRunFuture.get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    catch (ExecutionException | CancellationException ignored) {
      // Do nothing
    }
  }
}
