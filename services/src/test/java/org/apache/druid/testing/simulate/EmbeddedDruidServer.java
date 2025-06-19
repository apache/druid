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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.utils.RuntimeInfo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An embedded Druid server used in simulation tests.
 * This class and its methods (except a couple) are kept package protected as
 * they are used only by the specific server implementations.
 */
abstract class EmbeddedDruidServer implements EmbeddedServiceClientProvider
{
  private static final Logger log = new Logger(EmbeddedDruidServer.class);

  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final String name;

  private final ServiceClientHolder clientHolder = new ServiceClientHolder();

  EmbeddedDruidServer()
  {
    this.name = StringUtils.format(
        "%s-%d",
        this.getClass().getSimpleName(),
        SERVER_ID.incrementAndGet()
    );
  }

  /**
   * @return Name of this server = type + 2-digit ID.
   */
  public String getName()
  {
    return name;
  }

  @Override
  public CoordinatorClient leaderCoordinator()
  {
    return clientHolder.coordinator;
  }

  @Override
  public OverlordClient leaderOverlord()
  {
    return clientHolder.overlord;
  }

  @Override
  public BrokerClient anyBroker()
  {
    return clientHolder.broker;
  }

  /**
   * Creates a {@link ServerRunnable} corresponding to a specific Druid service.
   * Implementations of this class must try to avoid overriding any default
   * Druid module unless it is through extensions and enabled via Druid properties.
   * Such override should also be visible in the unit tests so that there is no
   * hidden config and the embedded cluster closely replicates a real cluster.
   */
  abstract ServerRunnable createRunnable(
      LifecycleInitHandler handler
  );

  /**
   * {@link RuntimeInfo} to use for this server.
   */
  abstract RuntimeInfo getRuntimeInfo();

  /**
   * Builds properties to be used in the {@code StartupInjectorBuilder} while
   * launching this server. This must be called only after the required resources,
   * test folder, zookeeper and metadata store have been initialized.
   */
  Properties buildStartupProperties(
      TestFolder testFolder,
      EmbeddedZookeeper zk
  )
  {
    final Properties serverProperties = new Properties();

    // Add properties for temporary directories used by the servers
    final String logsDirectory = testFolder.getOrCreateFolder("indexer-logs").getAbsolutePath();
    final String taskDirectory = testFolder.newFolder().getAbsolutePath();
    final String storageDirectory = testFolder.newFolder().getAbsolutePath();
    log.info(
        "Server[%s] using directories: task directory[%s], logs directory[%s], storage directory[%s].",
        name, taskDirectory, logsDirectory, storageDirectory
    );
    serverProperties.setProperty("druid.indexer.task.baseDir", taskDirectory);
    serverProperties.setProperty("druid.indexer.logs.directory", logsDirectory);
    serverProperties.setProperty("druid.storage.storageDirectory", storageDirectory);

    // Add properties for Zookeeper
    serverProperties.setProperty("druid.zk.service.host", zk.getConnectString());
    return serverProperties;
  }

  /**
   * This method return "read-only" modules that read the dependencies injected
   * into Druid. It should not return any module that overrides any behaviour of
   * the default Druid modules. This ensures that the embedded cluster remains
   * true to a real-life Druid cluster.
   *
   * @see LifecycleInitHandler#getInitModules()
   * @see #createRunnable(LifecycleInitHandler)
   */
  List<? extends Module> getInitModules()
  {
    final Module referenceHolderModule
        = binder -> binder.bind(ServiceClientHolder.class).toInstance(clientHolder);

    return List.of(referenceHolderModule);
  }

  /**
   * Handler used during initialization of the lifecycle of an embedded server.
   */
  interface LifecycleInitHandler
  {
    /**
     * @return Modules that should be used in {@link ServerRunnable#getModules()}.
     * This list contains modules that cannot be injected into the
     * {@code StartupInjectorBuilder} as they need dependencies that are only
     * bound later either in {@link ServerRunnable#getModules()} itself or via
     * the {@code CoreInjectorBuilder}.
     */
    List<? extends Module> getInitModules();

    /**
     * All implementations of {@link EmbeddedDruidServer} must call this method
     * from {@link ServerRunnable#initLifecycle(Injector)}.
     */
    void onLifecycleInit(Lifecycle lifecycle);
  }

  /**
   * Holder for the service client instances that are being used by this server.
   */
  private static class ServiceClientHolder
  {
    @Inject
    CoordinatorClient coordinator;

    @Inject
    OverlordClient overlord;

    @Inject
    BrokerClient broker;
  }
}
