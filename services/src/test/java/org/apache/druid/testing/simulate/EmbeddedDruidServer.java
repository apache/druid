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

import com.google.inject.Binder;
import com.google.inject.Injector;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.utils.RuntimeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An embedded Druid server used in simulation tests.
 * This class and most of its methods are kept package protected as they are used
 * only by the specific server implementations in the same package.
 */
public abstract class EmbeddedDruidServer implements ReferenceProvider
{
  private static final Logger log = new Logger(EmbeddedDruidServer.class);
  protected static final long MEM_100_MB = 100_000_000;

  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final String name;

  private final Map<String, String> serverProperties = new HashMap<>();
  private final ReferenceHolder clientHolder = new ReferenceHolder();

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
  public final String getName()
  {
    return name;
  }

  /**
   * Adds a property to this server. These properties correspond to the
   * {@code runtime.properties} file in a real Druid cluster, and override the
   * common properties specified via {@link EmbeddedDruidCluster#addCommonProperty}.
   */
  public final EmbeddedDruidServer addProperty(String key, String value)
  {
    serverProperties.put(key, value);
    return this;
  }

  /**
   * Creates a {@link ServerRunnable} corresponding to a specific Druid service.
   * Implementations of this class MUST NOT return a {@link ServerRunnable} that
   * overrides any default Druid behaviour so that the embedded cluster closely
   * replicates a real cluster. If an override is needed, it must be done using
   * extensions and Druid properties, which are visible to the unit test so that
   * there is no hidden config.
   *
   * @see EmbeddedDruidCluster#addExtension
   * @see EmbeddedDruidCluster#addCommonProperty
   * @see EmbeddedDruidServer#addProperty
   */
  abstract ServerRunnable createRunnable(
      LifecycleInitHandler handler
  );

  /**
   * {@link RuntimeInfo} to use for this server.
   *
   * @return {@link RuntimeInfo} with 2 processors and 100MB memory by default.
   */
  RuntimeInfo getRuntimeInfo()
  {
    return new DruidProcessingConfigTest.MockRuntimeInfo(2, MEM_100_MB, MEM_100_MB);
  }

  /**
   * Properties to be used in the {@code StartupInjectorBuilder} while launching
   * this server. This must be called only after all the resources required by
   * the Druid server have been initialized.
   */
  final Properties getStartupProperties(
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

    if (this instanceof EmbeddedHistorical) {
      serverProperties.setProperty(
          "druid.segmentCache.locations",
          StringUtils.format(
              "[{\"path\":\"%s\",\"maxSize\":\"%s\"}]",
              testFolder.newFolder().getAbsolutePath(),
              MEM_100_MB
          )
      );
    }

    serverProperties.putAll(this.serverProperties);
    return serverProperties;
  }

  /**
   * Binds the {@link ReferenceHolder} for this server.
   * All implementations of {@link EmbeddedDruidServer} must use this binding in
   * {@link ServerRunnable#getModules()}.
   */
  final void bindReferenceHolder(Binder binder)
  {
    binder.bind(ReferenceHolder.class).toInstance(clientHolder);
  }

  @Override
  public CoordinatorClient leaderCoordinator()
  {
    return clientHolder.leaderCoordinator();
  }

  @Override
  public OverlordClient leaderOverlord()
  {
    return clientHolder.leaderOverlord();
  }

  @Override
  public BrokerClient anyBroker()
  {
    return clientHolder.anyBroker();
  }

  @Override
  public LatchableEmitter emitter()
  {
    return clientHolder.emitter();
  }

  @Override
  public IndexerMetadataStorageCoordinator segmentsMetadataStorage()
  {
    return clientHolder.segmentsMetadataStorage();
  }

  /**
   * Handler used to register the lifecycle of an embedded server.
   */
  interface LifecycleInitHandler
  {
    /**
     * Registers the lifecycle of this server so that it can be stopped later.
     * All implementations of {@link EmbeddedDruidServer} must call this method
     * from {@link ServerRunnable#initLifecycle(Injector)}.
     */
    void onLifecycleInit(Lifecycle lifecycle);
  }
}
