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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.testing.simulate.derby.InMemoryDerbyModule;
import org.apache.druid.testing.simulate.derby.InMemoryDerbyResource;
import org.apache.druid.testing.simulate.emitter.LatchableEmitterModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Builder for an embedded Druid cluster that can be used in simulation tests.
 * <p>
 * A cluster is initialized with the following:
 * <ul>
 * <li>One or more {@link EmbeddedDruidServer}.</li>
 * <li>{@link TestFolder} to write segments, task logs, reports, etc.</li>
 * <li>An optional {@link EmbeddedZookeeper} server used by all the Druid services.</li>
 * <li>An optional in-memory Derby metadata store.</li>
 * <li>Other {@link EmbeddedResource} to be used in the cluster. For example,
 * an {@link InMemoryDerbyResource}.</li>
 * <li>List of {@link DruidModule} to load specific extensions, e.g. {@link InMemoryDerbyModule}.</li>
 * <li>{@link #addCommonProperty Common properties} that are applied to all Druid
 * services in the cluster.</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * final EmbeddedDruidCluster cluster =
 *        EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
 *                            .addServer(new EmbeddedOverlord())
 *                            .addServer(new EmbeddedIndexer())
 *                            .addCommonProperty("druid.emitter", "logging");
 * cluster.start();
 * cluster.leaderOverlord.runTask(...);
 * cluster.leaderOverlord().taskStatus(...);
 * cluster.stop();
 * </pre>
 */
public class EmbeddedDruidCluster implements ClusterReferencesProvider, EmbeddedResource
{
  private static final Logger log = new Logger(EmbeddedDruidCluster.class);

  private final TestFolder testFolder = new TestFolder();

  private final List<EmbeddedDruidServer> servers = new ArrayList<>();
  private final List<EmbeddedResource> resources = new ArrayList<>();
  private final List<Class<? extends DruidModule>> extensionModules = new ArrayList<>();
  private final Properties commonProperties = new Properties();

  private boolean started = false;
  private EmbeddedZookeeper zookeeper;

  private EmbeddedDruidCluster()
  {
    resources.add(testFolder);
  }

  /**
   * Creates a cluster with an in-memory Derby metadata store and an embedded
   * Zookeeper server.
   *
   * @see EmbeddedZookeeper
   * @see InMemoryDerbyModule
   */
  public static EmbeddedDruidCluster withEmbeddedDerbyAndZookeeper()
  {
    final EmbeddedDruidCluster cluster = new EmbeddedDruidCluster();
    cluster.resources.add(new InMemoryDerbyResource(cluster));
    cluster.extensionModules.add(InMemoryDerbyModule.class);
    cluster.addEmbeddedZookeeper();

    return cluster;
  }

  /**
   * Creates a new empty {@link EmbeddedDruidCluster} with no preloaded extensions
   * or resources. This method should be used when using non-embedded metadata
   * store and zookeeper, otherwise use {@link #withEmbeddedDerbyAndZookeeper()}.
   */
  public static EmbeddedDruidCluster empty()
  {
    return new EmbeddedDruidCluster();
  }

  private void addEmbeddedZookeeper()
  {
    this.zookeeper = new EmbeddedZookeeper();
    resources.add(zookeeper);
  }

  /**
   * Configures this cluster to use a {@link LatchableEmitter} by setting common
   * runtime property {@code druid.emitter=latching} and adding the extension
   * {@link LatchableEmitterModule}.
   */
  public EmbeddedDruidCluster useLatchableEmitter()
  {
    addCommonProperty("druid.emitter", LatchableEmitter.TYPE);
    extensionModules.add(LatchableEmitterModule.class);
    return this;
  }

  /**
   * Adds an extension to this cluster. The list of extensions is populated in
   * the common property {@code druid.extensions.modulesForSimulation}.
   * All extensions must be added before any server has been added to the cluster.
   */
  public EmbeddedDruidCluster addExtension(Class<? extends DruidModule> moduleClass)
  {
    validateNotStarted();
    if (!servers.isEmpty()) {
      throw new ISE("All extensions must be added to the cluster before adding any Druid server");
    }
    extensionModules.add(moduleClass);
    return this;
  }

  /**
   * Adds a Druid service to this cluster.
   */
  public EmbeddedDruidCluster addServer(EmbeddedDruidServer server)
  {
    validateNotStarted();
    servers.add(server);
    resources.add(new DruidServerResource(server, testFolder, zookeeper, commonProperties));
    return this;
  }

  /**
   * Adds a resource to this cluster. This method should not be used to add
   * Druid services to the cluster, use {@link #addServer} instead.
   * Resources and servers are started in the same order in which they are added
   * to the cluster using {@link #addServer} or this method.
   */
  public EmbeddedDruidCluster addResource(EmbeddedResource resource)
  {
    validateNotStarted();
    resources.add(resource);
    return this;
  }

  /**
   * Adds a property to be applied to all the Druid servers in this cluster.
   * These properties correspond to the {@code common.runtime.properties} file
   * used in a real Druid cluster. Each server can override these properties via
   * {@link EmbeddedDruidServer#addProperty}.
   */
  public EmbeddedDruidCluster addCommonProperty(String key, String value)
  {
    validateNotStarted();
    commonProperties.setProperty(key, value);
    return this;
  }

  /**
   * The test directory used by this cluster. Each Druid service creates a
   * sub-folder inside this directory to write out task logs or segments.
   */
  public TestFolder getTestFolder()
  {
    return testFolder;
  }

  /**
   * The embedded Zookeeper server used by this cluster, if any.
   *
   * @throws NullPointerException if this cluster has no embedded zookeeper.
   */
  public EmbeddedZookeeper getZookeeper()
  {
    return Objects.requireNonNull(zookeeper, "No embedded zookeeper configured for this cluster");
  }

  /**
   * Initializes all the resources used by this cluster. Typically invoked from
   * JUnit setup methods annotated with {@code Before} or {@code BeforeClass}.
   */
  @Override
  public void start() throws Exception
  {
    Preconditions.checkArgument(!servers.isEmpty(), "Cluster must have atleast one embedded Druid server");

    addCommonProperty("druid.extensions.modulesForSimulation", getExtensionModuleProperty());
    log.info("Starting cluster with common properties[%s].", commonProperties);

    // Start the resources in order
    for (EmbeddedResource resource : resources) {
      try {
        resource.start();
      }
      catch (Exception e) {
        // Clean up the resources that have already been started
        stop();
        throw e;
      }
    }

    // Mark the cluster as added so that no new resource, server or property is added
    started = true;
  }

  /**
   * Cleans up all the resources used by this cluster. Typically invoked from
   * JUnit tear down methods annotated with {@code After} or {@code AfterClass}.
   */
  @Override
  public void stop()
  {
    // Stop the resources in reverse order
    for (EmbeddedResource resource : Lists.reverse(resources)) {
      try {
        resource.stop();
      }
      catch (Exception e) {
        log.error(e, "Could not clean up resource[%s]. Continuing cleanup of other resources.", resource);
      }
    }
  }

  @Override
  public CoordinatorClient leaderCoordinator()
  {
    return servers.get(0).leaderCoordinator();
  }

  @Override
  public OverlordClient leaderOverlord()
  {
    return servers.get(0).leaderOverlord();
  }

  @Override
  public BrokerClient anyBroker()
  {
    return servers.get(0).anyBroker();
  }

  private void validateNotStarted()
  {
    if (started) {
      throw new ISE("Cluster has already started");
    }
  }

  private String getExtensionModuleProperty()
  {
    return getPropertyValue(
        extensionModules.stream().map(Class::getName).collect(Collectors.toList())
    );
  }

  private static String getPropertyValue(List<String> items)
  {
    final String csv = items.stream().map(name -> "\"" + name + "\"").collect(Collectors.joining(","));
    return "[" + csv + "]";
  }

}
