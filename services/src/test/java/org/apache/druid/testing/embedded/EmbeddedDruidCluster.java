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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.testing.embedded.derby.InMemoryDerbyModule;
import org.apache.druid.testing.embedded.derby.InMemoryDerbyResource;
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.utils.RuntimeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Builder for an embedded Druid cluster that can be used in embedded tests.
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
 * <li>{@link RuntimeInfoModule} supplying a {@link RuntimeInfo} with values matching
 * {@link EmbeddedDruidServer#setServerMemory} and {@link EmbeddedDruidServer#setServerDirectMemory}</li>
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
public class EmbeddedDruidCluster implements EmbeddedResource
{
  private static final Logger log = new Logger(EmbeddedDruidCluster.class);

  private final EmbeddedClusterApis clusterApis;
  private final TestFolder testFolder = new TestFolder();

  private final List<EmbeddedDruidServer<?>> servers = new ArrayList<>();
  private final List<EmbeddedResource> resources = new ArrayList<>();
  private final List<Class<? extends DruidModule>> extensionModules = new ArrayList<>();
  private final Properties commonProperties = new Properties();

  private EmbeddedHostname embeddedHostname = EmbeddedHostname.localhost();
  private boolean startedFirstDruidServer = false;

  private EmbeddedDruidCluster()
  {
    resources.add(testFolder);
    clusterApis = new EmbeddedClusterApis(this);
    addExtension(RuntimeInfoModule.class);
  }

  /**
   * Creates a cluster with an embedded Zookeeper server, but no particular
   * metadata store configured.
   *
   * @see EmbeddedZookeeper
   */
  public static EmbeddedDruidCluster withZookeeper()
  {
    return new EmbeddedDruidCluster().addResource(new EmbeddedZookeeper());
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
    final EmbeddedDruidCluster cluster = withZookeeper();
    cluster.resources.add(new InMemoryDerbyResource());
    cluster.extensionModules.add(InMemoryDerbyModule.class);

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

  /**
   * Configures this cluster to use a {@link LatchableEmitter}. This method is a
   * shorthand for the following:
   * <pre>
   * cluster.addCommonProperty("druid.emitter", "latching");
   * cluster.addExtension(LatchableEmitterModule.class);
   * </pre>
   *
   * @see LatchableEmitter
   * @see LatchableEmitterModule
   */
  public EmbeddedDruidCluster useLatchableEmitter()
  {
    addCommonProperty("druid.emitter", LatchableEmitter.TYPE);
    extensionModules.add(LatchableEmitterModule.class);
    return this;
  }

  /**
   * Configures this cluster to use the given default timeout with the
   * {@link LatchableEmitter}. Slow tests may set a higher value for the timeout
   * to avoid failures. If the cluster does not {@link #useLatchableEmitter()},
   * this method has no effect. Default timeout is 10 seconds.
   */
  public EmbeddedDruidCluster useDefaultTimeoutForLatchableEmitter(long timeoutSeconds)
  {
    addCommonProperty(
        "druid.emitter.latching.defaultWaitTimeoutMillis",
        String.valueOf(timeoutSeconds * 1000)
    );
    return this;
  }

  /**
   * Adds an extension to this cluster. The list of extensions is populated in
   * the common property {@code druid.extensions.modulesForEmbeddedTest}.
   */
  public EmbeddedDruidCluster addExtension(Class<? extends DruidModule> moduleClass)
  {
    validateNotStarted();
    extensionModules.add(moduleClass);
    return this;
  }

  /**
   * Adds extensions to this cluster.
   *
   * @see #addExtension(Class)
   */
  @SafeVarargs
  public final EmbeddedDruidCluster addExtensions(Class<? extends DruidModule>... moduleClasses)
  {
    validateNotStarted();
    extensionModules.addAll(List.of(moduleClasses));
    return this;
  }

  /**
   * Adds a Druid server to this cluster. A server added to the cluster after the
   * cluster has started must be started explicitly by calling
   * {@link EmbeddedDruidServer#start()}.
   */
  public EmbeddedDruidCluster addServer(EmbeddedDruidServer<?> server)
  {
    servers.add(server);
    resources.add(server);
    if (startedFirstDruidServer) {
      server.beforeStart(this);
    }
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

  public Properties getCommonProperties()
  {
    return commonProperties;
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
   * Uses a container-friendly hostname for all embedded services, Druid as well
   * as external.
   */
  public EmbeddedDruidCluster useContainerFriendlyHostname()
  {
    this.embeddedHostname = EmbeddedHostname.containerFriendly();
    return this;
  }

  /**
   * Hostname to be used for embedded services (both Druid or external).
   */
  public EmbeddedHostname getEmbeddedHostname()
  {
    return embeddedHostname;
  }

  /**
   * Initializes all the resources used by this cluster. Typically invoked from
   * JUnit setup methods annotated with {@code Before} or {@code BeforeClass}.
   */
  @Override
  public void start() throws Exception
  {
    Preconditions.checkArgument(!servers.isEmpty(), "Cluster must have at least one embedded Druid server");

    // Add clusterApis as the last entry in the resources list, so that the
    // EmbeddedServiceClient is initialized after mappers have been injected into the servers
    if (!startedFirstDruidServer) {
      resources.add(clusterApis);
    }

    // Start the resources in order
    for (EmbeddedResource resource : resources) {
      try {
        if (resource instanceof EmbeddedDruidServer<?> && !startedFirstDruidServer) {
          // Defer setting the extensions property until the first Druid server starts, so configureCluster calls for
          // earlier resources can add extensions.
          addCommonProperty("druid.extensions.modulesForEmbeddedTest", getExtensionModuleProperty());
          log.info("Starting Druid services with common properties[%s].", commonProperties);

          // Mark the cluster as started so that no new resource, server or property is added
          startedFirstDruidServer = true;
        }

        log.info("Starting resource[%s].", resource);
        resource.beforeStart(this);
        resource.start();
        resource.onStarted(this);
      }
      catch (Exception e) {
        log.warn(e, "Failed to start resource[%s]. Stopping cluster.", resource);
        // Clean up the resources that have already been started
        stop();
        throw e;
      }
    }
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
        log.info("Stopping resource[%s].", resource);
        resource.stop();
      }
      catch (Exception e) {
        log.error(e, "Could not clean up resource[%s]. Continuing cleanup of other resources.", resource);
      }
    }
  }

  /**
   * @return {@link EmbeddedClusterApis} to interact with this cluster.
   */
  public EmbeddedClusterApis callApi()
  {
    return clusterApis;
  }

  /**
   * Runs the given SQL on this cluster and returns the result as a single csv String.
   *
   * @see EmbeddedClusterApis#runSql(String, Object...)
   */
  public String runSql(String sql, Object... args)
  {
    return clusterApis.runSql(sql, args);
  }

  EmbeddedDruidServer<?> anyServer()
  {
    return servers.get(0);
  }

  <S extends EmbeddedDruidServer<S>> S findServerOfType(
      Class<S> serverType
  )
  {
    S foundServer = null;
    for (EmbeddedDruidServer<?> server : servers) {
      if (serverType.isInstance(server)) {
        foundServer = serverType.cast(server);
        break;
      }
    }

    return Objects.requireNonNull(
        foundServer,
        StringUtils.format("Cluster has no %s", serverType.getSimpleName())
    );
  }

  private void validateNotStarted()
  {
    if (startedFirstDruidServer) {
      throw new ISE("Cluster has already begun starting up");
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
