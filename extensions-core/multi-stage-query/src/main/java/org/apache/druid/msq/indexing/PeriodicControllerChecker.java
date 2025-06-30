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

package org.apache.druid.msq.indexing;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class PeriodicControllerChecker implements Closeable
{
  private static final Logger log = new Logger(PeriodicControllerChecker.class);
  private static final long FREQUENCY_CHECK_MILLIS = 1000;
  private static final long FREQUENCY_CHECK_JITTER = 30;

  private final String controllerId;
  private final String workerId;
  private final ServiceLocator controllerLocator;
  private final Runnable workerCanceler;
  private final ExecutorService exec;

  public PeriodicControllerChecker(
      final String controllerId,
      final String workerId,
      final ServiceLocator controllerLocator,
      final Runnable workerCanceler,
      final ExecutorService exec
  )
  {
    this.controllerId = controllerId;
    this.workerId = workerId;
    this.controllerLocator = controllerLocator;
    this.workerCanceler = workerCanceler;
    this.exec = exec;
  }

  /**
   * Create a new checker. This function is named "open" to emphasize that it is important to {@link #close()}
   * when done.
   *
   * @param controllerId      controller task ID
   * @param workerId          worker task ID
   * @param controllerLocator locator for the controller task, from {@link IndexerWorkerContext#controllerLocator()}
   * @param workerCanceler    runnable that cancels work
   */
  public static PeriodicControllerChecker open(
      final String controllerId,
      final String workerId,
      final ServiceLocator controllerLocator,
      final Runnable workerCanceler
  )
  {
    final PeriodicControllerChecker checker = new PeriodicControllerChecker(
        controllerId,
        workerId,
        controllerLocator,
        workerCanceler,
        Execs.singleThreaded("controller-status-checker[" + StringUtils.encodeForFormat(controllerId) + "]-%d")
    );
    checker.start();
    return checker;
  }

  void start()
  {
    exec.submit(this::controllerCheckerRunnable);
  }

  void controllerCheckerRunnable()
  {
    try {
      while (true) {
        // Add some randomness to the frequency of the loop to avoid requests from simultaneously spun up tasks bunching
        // up and stagger them randomly
        long sleepTimeMillis =
            FREQUENCY_CHECK_MILLIS
            + ThreadLocalRandom.current().nextLong(-FREQUENCY_CHECK_JITTER, 2 * FREQUENCY_CHECK_JITTER);
        final ServiceLocations controllerLocations = controllerLocator.locate().get();

        // Note: don't exit on empty location, because that may happen if the Overlord is slow to acknowledge the
        // location of a task. Only exit on "closed", because that happens only if the task is really no longer running.
        if (controllerLocations.isClosed()) {
          log.warn("Controller[%s] no longer exists. Worker[%s] will exit.", controllerId, workerId);
          break;
        }

        Thread.sleep(sleepTimeMillis);
      }
    }
    catch (InterruptedException ignored) {
      // Exit quietly. An interrupt means we were shut down intentionally.
      return;
    }
    catch (Throwable e) {
      // Service locator exceptions are not recoverable.
      log.warn(
          e,
          "Periodic fetch of controller[%s] location encountered an exception. Worker[%s] will exit.",
          controllerId,
          workerId
      );
    }

    // Cancel the worker.
    try {
      workerCanceler.run();
    }
    catch (Throwable e) {
      log.warn(e, "Failed to cancel worker[%s]", workerId);
    }
  }

  @Override
  public void close()
  {
    exec.shutdownNow();
  }
}
