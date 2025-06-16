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

import com.google.common.base.Preconditions;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builder for an embedded Druid cluster that can be used in simulation tests.
 * A cluster is initialized with the following:
 * <ul>
 * <li>One or more Druid servers</li>
 * <li>A single Zookeeper server used by all the Druid services</li>
 * <li>An optional in-memory Derby metadata store</li>
 * <li>Temporary folder for segment and task storage</li>
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
 * @see EmbeddedKafkaServer
 * @see TestDerbyConnector
 * @see EmbeddedDruidServer
 */
public class EmbeddedDruidCluster implements EmbeddedServiceClientProvider
{
  private final List<EmbeddedDruidServer> servers;
  private final EmbeddedZookeeper zookeeper;
  private final EmbeddedKafkaServer kafka;
  private final TemporaryFolder tempDir;
  private final TestDerbyConnector.DerbyConnectorRule dbRule;

  private EmbeddedDruidCluster(
      List<EmbeddedDruidServer> servers,
      boolean hasKafka,
      boolean hasMetadataStore
  )
  {
    this.tempDir = new TemporaryFolder();
    this.servers = List.copyOf(servers);
    this.zookeeper = new EmbeddedZookeeper();
    this.dbRule = hasMetadataStore ? new TestDerbyConnector.DerbyConnectorRule() : null;
    this.kafka = hasKafka ? new EmbeddedKafkaServer(zookeeper, tempDir, Map.of()) : null;
  }

  public static EmbeddedDruidCluster.Builder builder()
  {
    return new EmbeddedDruidCluster.Builder();
  }

  /**
   * Builds a {@link RuleChain} that can be used with JUnit {@code Rule} or
   * {@code ClassRule} to run simulation tests with this cluster.
   */
  public RuleChain ruleChain()
  {
    RuleChain ruleChain = RuleChain.outerRule(tempDir);
    ruleChain = ruleChain.around(zookeeper);

    if (dbRule != null) {
      ruleChain = ruleChain.around(dbRule);
    }
    if (kafka != null) {
      ruleChain = ruleChain.around(kafka);
    }

    for (EmbeddedDruidServer server : servers) {
      ruleChain = ruleChain.around(server.junitResource(tempDir, zookeeper, dbRule));
    }

    return ruleChain;
  }

  public EmbeddedKafkaServer kafkaServer()
  {
    return kafka;
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

  /**
   * Builder for an {@link EmbeddedDruidCluster}.
   */
  public static class Builder
  {
    private final List<EmbeddedDruidServer> servers = new ArrayList<>();
    private boolean hasMetadataStore = false;
    private boolean hasKafka = false;

    /**
     * Adds an in-memory Derby metadata store to this cluster.
     */
    public Builder withDb()
    {
      this.hasMetadataStore = true;
      return this;
    }

    /**
     * Adds an {@link EmbeddedKafkaServer} to this cluster.
     */
    public Builder withKafka()
    {
      this.hasKafka = true;
      return this;
    }

    /**
     * Adds a server to this cluster.
     */
    public Builder with(EmbeddedDruidServer server)
    {
      servers.add(server);
      return this;
    }

    public EmbeddedDruidCluster build()
    {
      Preconditions.checkArgument(!servers.isEmpty(), "Cluster must have atleast one embedded Druid server");
      return new EmbeddedDruidCluster(servers, hasKafka, hasMetadataStore);
    }
  }
}
