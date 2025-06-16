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

package org.apache.druid.testing.simulate.embedded;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.SQLMetadataStorageDruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.NoopMetadataStorageProvider;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageProvider;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.IOException;
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
   * launching this server.
   */
  Properties buildStartupProperties(
      TemporaryFolder tempDir,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  ) throws IOException
  {
    final Properties serverProperties = new Properties();

    // Add properties for temporary directories used by the servers
    final String logsDirectory = tempDir.getRoot().getAbsolutePath();
    final String taskDirectory = tempDir.newFolder().getAbsolutePath();
    final String storageDirectory = tempDir.newFolder().getAbsolutePath();
    log.info(
        "Server[%s] using directories: task directory[%s], logs directory[%s], storage directory[%s].",
        name, taskDirectory, logsDirectory, storageDirectory
    );
    serverProperties.setProperty("druid.indexer.task.baseDir", taskDirectory);
    serverProperties.setProperty("druid.indexer.logs.directory", logsDirectory);
    serverProperties.setProperty("druid.storage.storageDirectory", storageDirectory);

    // Add properties for Zookeeper and metadata store
    serverProperties.setProperty("druid.zk.service.host", zk.getConnectString());
    if (dbRule != null) {
      serverProperties.setProperty("druid.metadata.storage.type", TestDerbyModule.TYPE);
      serverProperties.setProperty(
          "druid.metadata.storage.tables.base",
          dbRule.getConnector().getMetadataTablesConfig().getBase()
      );
    }

    return serverProperties;
  }

  /**
   * @see LifecycleInitHandler#getInitModules()
   */
  List<? extends Module> getInitModules(
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  )
  {
    final Module referenceHolderModule
        = binder -> binder.bind(ServiceClientHolder.class).toInstance(clientHolder);

    return dbRule == null
           ? List.of(referenceHolderModule)
           : List.of(referenceHolderModule, new TestDerbyModule(dbRule.getConnector()));
  }

  /**
   * Creates a JUnit {@link ExternalResource} for this server that can be used
   * with {@code Rule}, {@code ClassRule} or in a {@code RuleChain}.
   */
  ExternalResource junitResource(
      TemporaryFolder tempDir,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  )
  {
    return new DruidServerJunitResource(this, tempDir, zk, dbRule);
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
   * Guice module to bind {@link SQLMetadataConnector} to {@link TestDerbyConnector}.
   * Used in Coordinator and Overlord simulations to connect to an in-memory Derby
   * database.
   */
  private static class TestDerbyModule extends SQLMetadataStorageDruidModule
  {
    public static final String TYPE = "derbyInMemory";
    private final TestDerbyConnector connector;

    public TestDerbyModule(TestDerbyConnector connector)
    {
      super(TYPE);
      this.connector = connector;
    }

    @Override
    public void configure(Binder binder)
    {
      super.configure(binder);

      binder.bind(MetadataStorage.class).toProvider(NoopMetadataStorageProvider.class);

      PolyBind.optionBinder(binder, Key.get(MetadataStorageProvider.class))
              .addBinding(TYPE)
              .to(DerbyMetadataStorageProvider.class)
              .in(LazySingleton.class);

      PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
              .addBinding(TYPE)
              .toInstance(connector);

      PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
              .addBinding(TYPE)
              .toInstance(connector);

      PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
              .addBinding(TYPE)
              .to(DerbyMetadataStorageActionHandlerFactory.class)
              .in(LazySingleton.class);
    }
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
