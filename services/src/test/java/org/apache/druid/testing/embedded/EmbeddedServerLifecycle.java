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

package org.apache.druid.testing.embedded;

import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lifecycle of an {@link EmbeddedDruidServer}.
 */
class EmbeddedServerLifecycle
{
  private static final Logger log = new Logger(EmbeddedServerLifecycle.class);

  private final EmbeddedDruidServer server;

  private final TestFolder testFolder;
  private final EmbeddedZookeeper zookeeper;
  private final Properties commonProperties;

  private ExecutorService executorService;
  private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>();

  EmbeddedServerLifecycle(
      EmbeddedDruidServer server,
      TestFolder testFolder,
      EmbeddedZookeeper zookeeper,
      Properties commonProperties
  )
  {
    this.server = server;
    this.zookeeper = zookeeper;
    this.testFolder = testFolder;
    this.commonProperties = commonProperties;
  }

  public void start() throws Exception
  {
    if (lifecycle.get() != null) {
      log.info("Server[%s] is already running.", server.getName());
      return;
    }

    log.info("Starting server[%s] with common properties[%s]...", server.getName(), commonProperties);

    // Create and start the ServerRunnable
    final CountDownLatch lifecycleCreated = new CountDownLatch(1);
    final ServerRunnable serverRunnable = server.createRunnable(
        lifecycle -> {
          lifecycleCreated.countDown();
          EmbeddedServerLifecycle.this.lifecycle.set(lifecycle);
        }
    );

    executorService = Execs.multiThreaded(1, "Lifecycle-" + server.getName());
    executorService.submit(() -> runServer(serverRunnable));

    // Wait for lifecycle to be created and started
    if (lifecycleCreated.await(10, TimeUnit.SECONDS)) {
      awaitLifecycleStart();
    } else {
      throw new ISE("Timed out waiting for lifecycle of server[%s] to be created", server.getName());
    }
  }

  public void stop()
  {
    log.info("Stopping server[%s] ...", server.getName());

    final Lifecycle lifecycle = this.lifecycle.getAndSet(null);
    if (lifecycle != null) {
      lifecycle.stop();
    }
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  /**
   * Waits for the server lifecycle created in {@link #start()} to start.
   */
  private void awaitLifecycleStart()
  {
    try {
      final CountDownLatch started = new CountDownLatch(1);

      lifecycle.get().addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              started.countDown();
            }

            @Override
            public void stop()
            {

            }
          }
      );

      if (started.await(10, TimeUnit.SECONDS)) {
        log.info("Server[%s] is now running.", server.getName());
      } else {
        throw new ISE("Timed out waiting for lifecycle of server[%s] to be started", server.getName());
      }
    }
    catch (Exception e) {
      log.error(e, "Exception while waiting for server[%s] to start.", server.getName());
    }
  }

  /**
   * Runs the server. This method must be invoked on {@link #executorService},
   * as it blocks and does not return until {@link #executorService} is terminated.
   */
  private void runServer(ServerRunnable runnable)
  {
    try {
      final Properties serverProperties = new Properties();
      serverProperties.putAll(commonProperties);
      serverProperties.putAll(server.getStartupProperties(testFolder, zookeeper));

      final Injector injector = new StartupInjectorBuilder()
          .withProperties(serverProperties)
          .withExtensions()
          .build();

      injector.injectMembers(runnable);
      runnable.run();
    }
    catch (Throwable e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof InterruptedException) {
        log.warn("Interrupted while running server[%s].", server.getName());
      } else {
        log.error(e, "Error while running server[%s]", server.getName());
      }
    }
    finally {
      log.info("Stopped server[%s].", server.getName());
    }
  }

  @Override
  public String toString()
  {
    return "DruidServerResource{" +
           "server=" + server.getName() +
           '}';
  }
}
