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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.consul.discovery.ConsulDiscoveryModule;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLMetadataStorageModule;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.consul.ConsulClusterResource;
import org.apache.druid.testing.embedded.consul.ConsulSecurityMode;
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.testing.embedded.indexing.IngestionSmokeTest;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.psql.PostgreSQLMetadataResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.time.Duration;

/**
 * Shared Consul discovery integration test wiring for the TLS variants.
 */
@Tag("docker-test")
@EnabledIfSystemProperty(named = "druid.testing.consul.enabled", matches = "true")
abstract class BaseConsulDiscoveryDockerTest extends IngestionSmokeTest
{
  protected ConsulClusterResource consulResource;

  protected abstract ConsulSecurityMode getConsulSecurityMode();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final PostgreSQLMetadataResource postgreSQLMetadataResource = new PostgreSQLMetadataResource();
    final MinIOStorageResource minIOStorageResource = new MinIOStorageResource();

    consulResource = new ConsulClusterResource(getConsulSecurityMode());

    EmbeddedDruidCluster cluster = EmbeddedDruidCluster
        .empty()
        .useDefaultTimeoutForLatchableEmitter(120)
        .addExtensions(
            KafkaIndexTaskModule.class,
            LatchableEmitterModule.class,
            PostgreSQLMetadataStorageModule.class,
            ConsulDiscoveryModule.class
        );

    cluster.addResource(postgreSQLMetadataResource)
           .addResource(minIOStorageResource)
           .addResource(consulResource)
           .addResource(kafkaServer);

    cluster.addCommonProperty("druid.discovery.type", "consul")
           .addCommonProperty("druid.coordinator.selector.type", "consul")
           .addCommonProperty("druid.indexer.selector.type", "consul")
           .addCommonProperty("druid.discovery.consul.watch.watchSeconds", "PT5S");

    cluster.addCommonProperty("druid.zk.service.enabled", "false")
           .addCommonProperty("druid.zk.service.host", "");

    cluster.addCommonProperty("druid.serverview.type", "http")
           .addCommonProperty("druid.coordinator.loadqueuepeon.type", "http")
           .addCommonProperty("druid.indexer.runner.type", "httpRemote");

    cluster.addCommonProperty("druid.emitter", "http")
           .addCommonProperty("druid.emitter.http.recipientBaseUrl", eventCollector.getMetricsUrl())
           .addCommonProperty("druid.emitter.http.flushMillis", "500");

    cluster.addResource(new EmbeddedResource()
    {
      @Override
      public void start()
      {
      }

      @Override
      public void stop()
      {
      }

      @Override
      public void beforeStart(EmbeddedDruidCluster cluster)
      {
        String psqlConnectURI = StringUtils.format(
            "jdbc:postgresql://localhost:%d/%s",
            postgreSQLMetadataResource.getContainer().getMappedPort(5432),
            postgreSQLMetadataResource.getDatabaseName()
        );

        String consulHost = "localhost";
        String consulPort = Integer.toString(consulResource.getMappedPort());
        String minioUrl = minIOStorageResource.getEndpointUrl();

        cluster.addCommonProperty("druid.metadata.storage.connector.connectURI", psqlConnectURI);
        cluster.addCommonProperty("druid.discovery.consul.connection.host", consulHost);
        cluster.addCommonProperty("druid.discovery.consul.connection.port", consulPort);
        cluster.addCommonProperty("druid.s3.endpoint.url", minioUrl);

        ConsulSecurityMode mode = getConsulSecurityMode();
        if (mode == ConsulSecurityMode.TLS || mode == ConsulSecurityMode.MTLS) {
          cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.protocol", "TLSv1.2");
          cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.trustStoreType", "PKCS12");
          cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.trustStorePath", consulResource.getTrustStorePath());
          cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.trustStorePassword", consulResource.getStorePassword());
          cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.validateHostnames", "false");

          if (mode == ConsulSecurityMode.MTLS) {
            cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.keyStoreType", "PKCS12");
            cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.keyStorePath", consulResource.getKeyStorePath());
            cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.certAlias", "client");
            cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.keyStorePassword", consulResource.getStorePassword());
            cluster.addCommonProperty("druid.discovery.consul.connection.sslClientConfig.keyManagerPassword", consulResource.getStorePassword());
          }
        }
      }

      @Override
      public void onStarted(EmbeddedDruidCluster cluster)
      {
      }
    });

    cluster = addServers(cluster);

    return cluster;
  }

  @Override
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    return cluster
        .addServer(eventCollector)
        .addServer(new EmbeddedCoordinator())
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedRouter());
  }

  @BeforeEach
  public void verifyOverlordLeader()
  {
    Assertions.assertTrue(
        overlord.bindings().overlordLeaderSelector().isLeader(),
        "Embedded Overlord should be the leader"
    );
  }

  @Test
  public void testConsulExposesLeadershipKVs() throws Exception
  {
    final String coordinatorLeader = fetchConsulRawValue("druid/leader/coordinator");
    final String overlordLeader = fetchConsulRawValue("druid/leader/overlord");

    Assertions.assertTrue(coordinatorLeader.contains(":8081"), "Coordinator leader payload: " + coordinatorLeader);
    Assertions.assertTrue(overlordLeader.contains(":8090"), "Overlord leader payload: " + overlordLeader);
  }

  protected String fetchConsulRawValue(String key) throws Exception
  {
    return RetryUtils.retry(
        () -> {
          HttpClient client = createHttpClient();
          final HttpRequest request = HttpRequest.newBuilder(
                  consulResource.getHttpUri(StringUtils.format("/v1/kv/%s?raw", key))
              )
              .timeout(Duration.ofSeconds(5))
              .GET()
              .build();
          final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
          if (response.statusCode() != 200) {
            throw new IllegalStateException(
                StringUtils.format("Consul returned status[%d] for key[%s]", response.statusCode(), key)
            );
          }
          return response.body();
        },
        ex -> true,
        10
    );
  }

  /**
   * Creates an HTTP client with proper SSL configuration using the test trust store.
   * For TLS/mTLS modes, this loads the actual trust store used by the Consul test container.
   */
  private HttpClient createHttpClient() throws Exception
  {
    if (getConsulSecurityMode() == ConsulSecurityMode.PLAIN) {
      return HttpClient.newBuilder()
                       .connectTimeout(Duration.ofSeconds(5))
                       .build();
    }

    // For TLS/mTLS, load the actual trust store with the Consul CA certificate
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    try (FileInputStream fis = new FileInputStream(consulResource.getTrustStorePath())) {
      trustStore.load(fis, consulResource.getStorePassword().toCharArray());
    }

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);

    return HttpClient.newBuilder()
                     .sslContext(sslContext)
                     .connectTimeout(Duration.ofSeconds(5))
                     .build();
  }
}
