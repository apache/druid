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

package org.apache.druid.testing.simulate;

import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.utils.JvmUtils;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * {@link EmbeddedResource} for an {@link EmbeddedDruidServer}.
 * Handles the lifecycle of the server.
 */
class DruidServerResource implements EmbeddedResource
{
  private static final Logger log = new Logger(DruidServerResource.class);

  private final EmbeddedDruidServer server;

  private final TestFolder testFolder;
  private final EmbeddedZookeeper zk;
  private final TestDerbyConnector.DerbyConnectorRule dbRule;
  private final List<Class<? extends DruidModule>> extensionModules;

  private ExecutorService executorService;
  private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>();

  DruidServerResource(
      EmbeddedDruidServer server,
      TestFolder testFolder,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule,
      List<Class<? extends DruidModule>> extensionModules
  )
  {
    this.server = server;
    this.zk = zk;
    this.dbRule = dbRule;
    this.testFolder = testFolder;
    this.extensionModules = extensionModules;
  }

  @Override
  public void before() throws Exception
  {
    log.info("Starting server[%s] with extensions[%s] ...", server.getName(), extensionModules);

    // Create and start the ServerRunnable
    final CountDownLatch lifecycleCreated = new CountDownLatch(1);
    final ServerRunnable serverRunnable = server.createRunnable(
        new EmbeddedDruidServer.LifecycleInitHandler()
        {
          @Override
          public List<? extends Module> getInitModules()
          {
            return server.getInitModules(dbRule);
          }

          @Override
          public void onLifecycleInit(Lifecycle lifecycle)
          {
            lifecycleCreated.countDown();
            DruidServerResource.this.lifecycle.set(lifecycle);
          }
        }
    );

    executorService = Execs.multiThreaded(1, "Lifecycle-" + server.getName());
    executorService.submit(() -> runServer(serverRunnable));

    // Wait for lifecycle to be created and started
    lifecycleCreated.await();
    awaitLifecycleStart();
  }

  @Override
  public void after()
  {
    log.info("Stopping server[%s] ...", server.getName());

    lifecycle.get().stop();
    lifecycle.set(null);
    executorService.shutdownNow();
    executorService = null;
  }

  /**
   * Waits for the server lifecycle created in {@link #before()} to start.
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

      started.await();
      log.info("Server[%s] is now running.", server.getName());
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
      serverProperties.setProperty("druid.extensions.modulesForSimulation", getExtensionModuleProperty());
      serverProperties.putAll(server.buildStartupProperties(testFolder, zk, dbRule));

      final Injector injector = new StartupInjectorBuilder()
          .withProperties(serverProperties)
          .withExtensions()
          .add(binder -> {
            binder.bind(RuntimeInfo.class).toInstance(server.getRuntimeInfo());
            binder.requestStaticInjection(JvmUtils.class);
          })
          .build();

      injector.injectMembers(runnable);
      runnable.run();
    }
    catch (Exception e) {
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

  private String getExtensionModuleProperty()
  {
    final String moduleNamesCsv = extensionModules.stream()
                                                  .map(Class::getName)
                                                  .map(name -> "\"" + name + "\"")
                                                  .collect(Collectors.joining(","));
    return "[" + moduleNamesCsv + "]";
  }
}
