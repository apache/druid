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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.docker.LatestImageDockerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DruidKubernetesBaseDockerTest extends KubernetesTestBase implements LatestImageDockerTest
{
  private static final Logger log = new Logger(DruidKubernetesBaseDockerTest.class);
  private static final String DRUID_NAMESPACE = "druid";
  private static final String DRUID_DOCKER_TEST_IMAGE = "apache/druid:docker-tests";
  private static final String PROPERTY_TEST_IMAGE = "druid.testing.docker.image";

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

    druidCluster = new DruidClusterComponent(DRUID_NAMESPACE, localImageName, clusterName, getDeployingResource());

    druidCluster.addDruidService(new DruidK8sHistoricalComponent(
        DRUID_NAMESPACE,
        localImageName,
        clusterName,
        "hot",
        1,
        getDeployingResource().getTestFolder().getOrCreateFolder("druid-storage/segments").getAbsolutePath()
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
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  public void test_cluster_fullIngestionAndQuery() throws Exception
  {
    String dataSource = "test_datasource_" + StringUtils.replace(UUID.randomUUID().toString(), "-", "");
    druidCluster.setLoadRule(dataSource, "hot", 2);
    String taskJson = StringUtils.format("{\n" +
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
    String brokerUrl = druidCluster.getBrokerExternalUrl().get();
    String datasourcesUrl = brokerUrl + "/druid/v2/datasources";

    boolean datasourceAvailable = false;
    for (int attempt = 1; attempt <= 20; attempt++) {
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

      if (attempt < 20) {
        Thread.sleep(10000);
      }
    }
    Assertions.assertTrue(datasourceAvailable, "Datasource should be queryable within timeout");
  }
}
