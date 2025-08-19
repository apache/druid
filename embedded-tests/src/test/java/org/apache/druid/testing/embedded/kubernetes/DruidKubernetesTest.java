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

package org.apache.druid.testing.embedded.kubernetes;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.TestFolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.k3s.K3sContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DruidKubernetesTest extends KubernetesTestBase
{
  private static final Logger log = new Logger(DruidKubernetesTest.class);
  private static final String DRUID_NAMESPACE = "druid";
  private static final String DRUID_DOCKER_TEST_IMAGE = "apache/druid:docker-tests";
  private static final String PROPERTY_TEST_IMAGE = "druid.testing.docker.image";
  private static final String DRUID_SEGMENT_DIR = "/tmp/druid/segments";

  private static DruidOperatorComponent druidOperator;
  private DruidClusterComponent druidCluster;

  @BeforeAll
  public static void init()
  {
    startK3SContainer();

    String localImageName = System.getProperty(PROPERTY_TEST_IMAGE, DRUID_DOCKER_TEST_IMAGE);
    getDeployingResource().loadLocalImage(localImageName);
    
    createNamespace(DRUID_NAMESPACE);
    druidOperator = new DruidOperatorComponent(DRUID_NAMESPACE);
    druidOperator.setK3SResource(getDeployingResource());
    addKubernetesComponent(druidOperator, false);
    initializeComponents();
  }

  @BeforeEach
  public void setUp()
  {
    String clusterName = "druid-it";
    String localImageName = System.getProperty(PROPERTY_TEST_IMAGE, DRUID_DOCKER_TEST_IMAGE);
    TestFolder testFolder = getDeployingResource().getTestFolder();

    testFolder.getOrCreateFolder(DRUID_SEGMENT_DIR);
    druidCluster = new DruidClusterComponent(DRUID_NAMESPACE, localImageName, clusterName, getDeployingResource());

    druidCluster.addDruidService(new DruidK8sHistoricalComponent(
        DRUID_NAMESPACE,
        localImageName,
        clusterName,
        "hot",
        1,
        DRUID_SEGMENT_DIR
    ));
    druidCluster.addDruidService(new DruidK8sRouterComponent(
        DRUID_NAMESPACE,
        localImageName,
        clusterName
    ));
    druidCluster.addDruidService(new DruidK8sBrokerComponent(
        DRUID_NAMESPACE,
        localImageName,
        clusterName
    ));
    druidCluster.addDruidService(new DruidK8sCoordinatorComponent(
        DRUID_NAMESPACE,
        localImageName,
        clusterName
    ));
    
    addKubernetesComponent(druidCluster);
    initializeComponents();
  }

  @AfterEach
  void tearDown()
  {
    cleanupComponents(false);

    try {
      Thread.sleep(5000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @AfterAll
  public static void cleanup()
  {
    cleanupComponents(true);
    stopK3SContainer();
  }

  @Test
  public void test_operator_deployment()
  {
    Deployment deployment = getClient().apps().deployments()
                                       .inNamespace(druidOperator.getNamespace())
                                       .withName("druid-operator-test")
                                       .get();

    if (deployment == null) {
      throw new AssertionError("Druid operator deployment not found");
    }

    Assertions.assertNotNull(deployment.getStatus().getReadyReplicas());
    Assertions.assertTrue(
        deployment.getStatus().getReadyReplicas() >= 1,
        "Druid operator deployment should have at least 1 ready replica"
    );
  }

  @Test
  public void test_operator_namespace_watching()
  {
    Deployment deployment = getClient().apps().deployments()
                                       .inNamespace(druidOperator.getNamespace())
                                       .withName("druid-operator-test")
                                       .get();

    if (deployment == null) {
      throw new AssertionError("Druid operator deployment not found");
    }

    List<Container> containers = deployment.getSpec().getTemplate().getSpec().getContainers();
    containers.stream()
              .filter(container -> "manager".equals(container.getName()) && container.getEnv() != null)
              .flatMap(container -> container.getEnv().stream())
              .filter(envVar -> "WATCH_NAMESPACE".equals(envVar.getName()))
              .findFirst()
              .ifPresent(envVar -> Assertions.assertEquals(DRUID_NAMESPACE, envVar.getValue()));
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  public void test_cluster_deployment()
  {
    for (DruidK8sComponent service : druidCluster.getDruidServices()) {
      String uniqueLabel = service.getPodLabel();
      AtomicBoolean found = new AtomicBoolean(false);
      StatefulSetList statefulSetsByLabel = getClient().apps().statefulSets()
                                                       .inNamespace(DRUID_NAMESPACE)
                                                       .withLabel("nodeSpecUniqueStr", uniqueLabel)
                                                       .list();

      statefulSetsByLabel.getItems().stream()
                         .findFirst()
                         .ifPresent(statefulSet -> {
                           found.set(true);
                           Assertions.assertNotNull(
                               statefulSet.getStatus().getReadyReplicas(),
                               "ReadyReplicas should not be null for " + service.getDruidServiceType()
                           );
                           Assertions.assertTrue(
                               statefulSet.getStatus().getReadyReplicas() >= 1,
                               "Druid " + service.getDruidServiceType() + " statefulset is not ready. Ready replicas: "
                               + statefulSet.getStatus().getReadyReplicas()
                           );
                         });
      if (!found.get()) {
        throw new AssertionError("Druid "
                                 + service.getDruidServiceType()
                                 + " statefulset not found by nodeSpecUniqueStr label: "
                                 + uniqueLabel);
      }
    }
  }

  @Test
  @Timeout(value = 50, unit = TimeUnit.MINUTES)
  public void test_cluster_fullIngestionAndQuery() throws Exception
  {
    String dataSource = "test_datasource_" + UUID.randomUUID().toString().replace("-", "");
    String taskJson = String.format("{\n" +
        "  \"type\": \"index_parallel\",\n" +
        "  \"spec\": {\n" +
        "    \"dataSchema\": {\n" +
        "      \"dataSource\": \"%s\",\n" +
        "      \"timestampSpec\": {\n" +
        "        \"column\": \"timestamp\",\n" +
        "        \"format\": \"auto\"\n" +
        "      },\n" +
        "      \"dimensionsSpec\": {\n" +
        "        \"dimensions\": [\"name\", \"value\"]\n" +
        "      },\n" +
        "      \"granularitySpec\": {\n" +
        "        \"type\": \"uniform\",\n" +
        "        \"segmentGranularity\": \"DAY\",\n" +
        "        \"queryGranularity\": \"HOUR\",\n" +
        "        \"rollup\": false\n" +
        "      }\n" +
        "    },\n" +
        "    \"ioConfig\": {\n" +
        "      \"type\": \"index_parallel\",\n" +
        "      \"inputSource\": {\n" +
        "        \"type\": \"inline\",\n" +
        "        \"data\": \"timestamp,name,value\\n2023-01-01T00:00:00Z,test1,100\\n2023-01-01T01:00:00Z,test2,200\\n2023-01-01T02:00:00Z,test3,300\"\n" +
        "      },\n" +
        "      \"inputFormat\": {\n" +
        "        \"type\": \"csv\",\n" +
        "        \"findColumnsFromHeader\": true\n" +
        "      }\n" +
        "    },\n" +
        "    \"tuningConfig\": {\n" +
        "      \"type\": \"index_parallel\",\n" +
        "      \"maxNumConcurrentSubTasks\": 1,\n" +
        "      \"maxRowsInMemory\": 1000\n" +
        "    }\n" +
        "  }\n" +
        "}", dataSource);
    
    String taskId = druidCluster.submitTask(taskJson);

    log.debug("Task submitted with ID: %s%n", taskId);
    druidCluster.waitUntilTaskCompletes(taskId);

    HttpClient httpClient = HttpClient.newBuilder()
                                      .connectTimeout(Duration.ofSeconds(30))
                                      .build();

    DruidK8sBrokerComponent broker = null;
    for (DruidK8sComponent service : druidCluster.getDruidServices()) {
      if (service instanceof DruidK8sBrokerComponent) {
        broker = (DruidK8sBrokerComponent) service;
        break;
      }
    }
    
    Assertions.assertNotNull(broker, "Broker service should be available");
    String brokerUrl = broker.getExternalUrl(getDeployingResource().getK3sContainer());
    String datasourcesUrl = brokerUrl + "/druid/v2/datasources";
    
    boolean datasourceAvailable = false;
    for (int attempt = 1; attempt <= 200; attempt++) {
      HttpRequest datasourcesRequest = HttpRequest.newBuilder()
          .uri(URI.create(datasourcesUrl))
          .header("Accept", "application/json")
          .GET()
          .timeout(Duration.ofSeconds(30))
          .build();
      
      HttpResponse<String> datasourcesResponse = httpClient.send(datasourcesRequest, HttpResponse.BodyHandlers.ofString());
      
      if (datasourcesResponse.statusCode() == 200) {
        String body = datasourcesResponse.body();
        log.debug("Attempt %d - Available datasources: %s%n", attempt, body);
        
        if (body.contains(dataSource)) {
          datasourceAvailable = true;
          break;
        }
      }
      
      if (attempt < 200) {
        Thread.sleep(10000);
      }
    }
    
    Assertions.assertTrue(datasourceAvailable, "Datasource should appear in broker within timeout");

    String queryJson = String.format("{\n" +
        "  \"queryType\": \"scan\",\n" +
        "  \"dataSource\": \"%s\",\n" +
        "  \"intervals\": [\"2023-01-01/2023-01-02\"],\n" +
        "  \"columns\": [\"__time\", \"name\", \"value\"],\n" +
        "  \"limit\": 10\n" +
        "}", dataSource);
    
    String queryUrl = brokerUrl + "/druid/v2/?pretty";
    
    HttpRequest queryRequest = HttpRequest.newBuilder()
        .uri(URI.create(queryUrl))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(queryJson))
        .timeout(Duration.ofSeconds(30))
        .build();
    
    HttpResponse<String> queryResponse = httpClient.send(queryRequest, HttpResponse.BodyHandlers.ofString());
    
    log.debug("Query response: %d%n", queryResponse.statusCode());
    log.debug("Query results: %s%n", queryResponse.body());
    
    Assertions.assertEquals(200, queryResponse.statusCode(), "Query should return 200");
    
    String queryResult = queryResponse.body();
    Assertions.assertTrue(queryResult.contains("test1") || queryResult.contains("test2") || queryResult.contains("test3"), 
        "Query results should contain our test data");
    
    System.out.println("✓ Successfully queried indexed data!");
    System.out.println("✓ Full ingestion and query pipeline working in K8s!");
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void test_druid_services_health() throws Exception
  {
    // Show service to port mapping
    Map<String, Integer> portMapping = druidCluster.getServicePortMapping();
    System.out.println("\n=== Dynamic Port Allocation ===");
    portMapping.forEach((service, port) -> 
        System.out.printf("Service: %s -> NodePort: %d%n", service, port));
    
    System.out.println("\n=== K3s NodePort → Localhost Port Mapping ===");
    K3sContainer k3sContainer = getDeployingResource().getK3sContainer();
    portMapping.forEach((service, nodePort) -> {
      try {
        int localhostPort = k3sContainer.getMappedPort(nodePort);
        System.out.printf("K3s NodePort %d → localhost:%d (for %s)%n", 
            nodePort, localhostPort, service);
      } catch (Exception e) {
        System.out.printf("K3s NodePort %d → ERROR: %s (for %s)%n", 
            nodePort, e.getMessage(), service);
      }
    });
    
    System.out.println("\n=== External URLs (accessible from localhost) ===");
    
    for (DruidK8sComponent service : druidCluster.getDruidServices()) {
      String externalUrl = service.getExternalUrl(getDeployingResource().getK3sContainer());
      String healthUrl = externalUrl + "/status/health";
      
      System.out.printf("%s service accessible at: %s%n", 
          service.getDruidServiceType().toUpperCase(), externalUrl);
      System.out.printf("  Health check: %s%n", healthUrl);
      
      // Perform health check with retry logic
      HttpResponse<String> response = null;
      Exception lastException = null;
      
      for (int attempt = 1; attempt <= 10; attempt++) {
        try {
          // Create fresh HttpClient for each attempt to avoid connection reuse issues
          HttpClient freshClient = HttpClient.newBuilder()
              .connectTimeout(Duration.ofSeconds(60))
              .build();
              
          HttpRequest healthRequest = HttpRequest.newBuilder()
              .uri(URI.create(healthUrl))
              .header("User-Agent", "DruidTest/1.0")
              .header("Accept", "application/json")
              .GET()
              .timeout(Duration.ofSeconds(60))
              .build();
          
          response = freshClient.send(healthRequest, HttpResponse.BodyHandlers.ofString());
          break; // Success, exit retry loop
          
        } catch (Exception e) {
          lastException = e;
          System.out.printf("  Attempt %d failed: %s%n", attempt, e.getMessage());
          if (attempt < 5) {
            Thread.sleep(10000); // Wait 2 seconds before retry
          }
        }
      }
      
      if (response == null) {
        throw new RuntimeException(String.format("%s service health check failed after 3 attempts", 
            service.getDruidServiceType()), lastException);
      }
      
      Assertions.assertEquals(200, response.statusCode(), 
          String.format("%s service health check failed with status %d", 
              service.getDruidServiceType(), response.statusCode()));
      
      System.out.printf("  ✓ Health check passed (HTTP %d)%n%n", response.statusCode());
    }
  }
}