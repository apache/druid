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
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.testing.simulate.derby.InMemoryDerbyModule;
import org.apache.druid.testing.simulate.derby.InMemoryDerbyResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Builder for an embedded Druid cluster that can be used in simulation tests.
 * <p>
 * A cluster is initialized with the following:
 * <ul>
 * <li>One or more {@link EmbeddedDruidServer}</li>
 * <li>{@link TestFolder} to write segments, task logs, reports, etc.</li>
 * <li>A single {@link EmbeddedZookeeper} server used by all the Druid services</li>
 * <li>An optional in-memory Derby metadata store</li>
 * <li>Other {@link EmbeddedResource} to be used in the cluster. For example,
 * an {@code EmbeddedKafkaServer}</li>
 * <li>List of {@link DruidModule} to load specific extensions</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 * private final EmbeddedOverlord overlord = EmbeddedOverlord.create();
 * private final EmbeddedIndexer indexer = EmbeddedIndexer.create();
 *
 * &#64;Rule
 * public RuleChain cluster = EmbeddedDruidCluster.builder()
 *                                                .withDb()
 *                                                .with(overlord)
 *                                                .with(indexer)
 *                                                .build();
 * </pre>
 *
 * @see EmbeddedZookeeper
 * @see TestDerbyConnector
 * @see EmbeddedDruidServer
 */
public class EmbeddedDruidCluster implements EmbeddedServiceClientProvider, EmbeddedResource
{
  private static final Logger log = new Logger(EmbeddedDruidCluster.class);

  private final TestFolder testFolder = new TestFolder();
  private final EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();

  private final List<EmbeddedDruidServer> servers = new ArrayList<>();
  private final List<EmbeddedResource> resources = new ArrayList<>();
  private final List<Class<? extends DruidModule>> extensionModules = new ArrayList<>();
  private final Properties commonProperties = new Properties();

  private boolean started = false;

  private EmbeddedDruidCluster(boolean hasMetadataStore)
  {
    resources.add(testFolder);
    resources.add(zookeeper);

    addCommonProperty("druid.emitter", StubServiceEmitter.TYPE);
    extensionModules.add(StubServiceEmitterModule.class);

    if (hasMetadataStore) {
      resources.add(new InMemoryDerbyResource(this));
      extensionModules.add(InMemoryDerbyModule.class);
    }
  }

  public static EmbeddedDruidCluster create()
  {
    return new EmbeddedDruidCluster(true);
  }

  /**
   * Creates a cluster with the given extensions. The list of extensions is
   * populated in the property {@code druid.extensions.modulesForSimulation}.
   */
  public static EmbeddedDruidCluster withExtensions(List<Class<? extends DruidModule>> moduleClasses)
  {
    final EmbeddedDruidCluster cluster = new EmbeddedDruidCluster(true);
    cluster.extensionModules.addAll(moduleClasses);
    return cluster;
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
   * These properties can be overridden by service-specific properties.
   */
  public EmbeddedDruidCluster addCommonProperty(String key, String value)
  {
    validateNotStarted();
    commonProperties.setProperty(key, value);
    return this;
  }

  public TestFolder getTestFolder()
  {
    return testFolder;
  }

  public EmbeddedZookeeper getZookeeper()
  {
    return zookeeper;
  }

  /**
   * Initializes all the resources used by this cluster. Typically invoked from
   * JUnit setup methods annotated with {@code Before} or {@code BeforeClass}.
   */
  @Override
  public void before() throws Exception
  {
    Preconditions.checkArgument(!servers.isEmpty(), "Cluster must have atleast one embedded Druid server");

    addCommonProperty("druid.extensions.modulesForSimulation", getExtensionModuleProperty());
    log.info("Starting cluster with common properties[%s].", commonProperties);

    // Start the resources in order
    for (EmbeddedResource resource : resources) {
      try {
        resource.before();
      }
      catch (Exception e) {
        // Clean up the resources that have already been started
        after();
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
  public void after()
  {
    // Stop the resources in reverse order
    for (EmbeddedResource resource : Lists.reverse(resources)) {
      try {
        resource.after();
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

  @Override
  public StubServiceEmitter serviceEmitter()
  {
    throw new ISE("There is no cluster-level service emitter. Use service specific emitters instead.");
  }

  private void validateNotStarted()
  {
    if (started) {
      throw new ISE("Cluster has already started");
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
