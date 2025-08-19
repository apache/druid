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

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.simulate.K3SResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.utils.PortAllocator;
import org.junit.jupiter.api.Assertions;

import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Component that orchestrates a complete Druid cluster deployment.
 * Manages all Druid services and their dependencies.
 */
public class DruidClusterComponent implements K8sComponent
{
  private static final Logger log = new Logger(DruidClusterComponent.class);

  private static final String COMPONENT_NAME = "DruidCluster";
  private static final String RBAC_MANIFEST_PATH = "data/manifests/druid-common/rbac.yaml";

  private final String namespace;
  private final String druidImage;
  private final String clusterName;
  private final List<DruidK8sComponent> druidServices = new ArrayList<>();
  private K3SResource k3sResource;
  private final PortAllocator portAllocator;

  public DruidClusterComponent(String namespace, String druidImage, String clusterName, K3SResource k3sResource)
  {
    this.namespace = namespace;
    this.druidImage = druidImage;
    this.clusterName = clusterName;
    this.k3sResource = k3sResource;
    this.portAllocator = new PortAllocator(30080, 30100);
  }


  public void addDruidService(DruidK8sComponent service)
  {
    String serviceKey = service.getPodLabel();
    int allocatedPort = portAllocator.allocatePort(serviceKey);
    service.setAllocatedNodePort(allocatedPort);
    
    // Share the TestFolder from K3SResource with the DruidK8sComponent
    if (k3sResource != null && k3sResource.getTestFolder() != null) {
      service.setTestFolder(k3sResource.getTestFolder());
    }

    druidServices.add(service);
    log.info(
        "Added Druid service %s with allocated NodePort %d",
        service.getDruidServiceType(), allocatedPort
    );
  }

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    log.info("Initializing %s...", getComponentName());
    applyRBACManifests(client);
    applyDruidClusterManifest(client);
    log.info("%s initialization completed", getComponentName());
  }

  @Override
  public void waitUntilReady(KubernetesClient client) throws Exception
  {
    log.info("Waiting for %s to be ready...", getComponentName());

    log.info("Allowing 2 seconds for all Druid services to start and discover each other...");
    Thread.sleep(2000);

    for (DruidK8sComponent service : druidServices) {
      log.info("Waiting for Druid %s to be ready...", service.getDruidServiceType());
      service.waitUntilReady(client);
    }

    log.info("%s is ready - all services are healthy!", getComponentName());
  }


  @Override
  public void cleanup(KubernetesClient client)
  {
    log.info("Cleaning up %s...", getComponentName());

    for (DruidK8sComponent service : druidServices) {
      try {
        service.cleanup(client);
        String serviceKey = service.getDruidServiceType() + "-" + clusterName;
        portAllocator.releasePort(serviceKey);
      }
      catch (Exception e) {
        log.error("Error cleaning up %s: %s", service.getDruidServiceType(), e.getMessage());
      }
    }

    cleanupRBACResources(client);
    log.info("%s cleanup completed", getComponentName());
  }

  @Override
  public String getComponentName()
  {
    return COMPONENT_NAME;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public List<DruidK8sComponent> getDruidServices()
  {
    return new ArrayList<>(druidServices);
  }

  public Optional<DruidK8sComponent> getCoordinator()
  {
    return druidServices.stream()
                        .filter(service -> "coordinator".equals(service.getDruidServiceType()))
                        .findFirst();
  }

  public Optional<DruidK8sComponent> getBroker()
  {
    return druidServices.stream()
                        .filter(service -> "broker".equals(service.getDruidServiceType()))
                        .findFirst();
  }

  public Optional<DruidK8sComponent> getRouter()
  {
    return druidServices.stream()
                        .filter(service -> "router".equals(service.getDruidServiceType()))
                        .findFirst();
  }

  public List<DruidK8sHistoricalComponent> getHistoricals()
  {
    return druidServices.stream()
                        .filter(service -> "historical".equals(service.getDruidServiceType()))
                        .map(service -> (DruidK8sHistoricalComponent) service)
                        .collect(Collectors.toList());
  }

  /**
   * Get all allocated ports for K3S container exposure.
   */
  public int[] getAllocatedPorts()
  {
    return portAllocator.getAllocatedPorts();
  }

  /**
   * Get service to port mapping for debugging.
   */
  public Map<String, Integer> getServicePortMapping()
  {
    return portAllocator.getServicePortMapping();
  }

  public Optional<String> getBrokerUrl()
  {
    return getBroker().map(DruidK8sComponent::getServiceUrl);
  }

  public Optional<String> getRouterUrl()
  {
    return getRouter().map(DruidK8sComponent::getServiceUrl);
  }

  /**
   * Get external coordinator URL for test connectivity.
   */
  public Optional<String> getCoordinatorExternalUrl()
  {
    return getCoordinator().map(coordinator -> coordinator.getExternalUrl(k3sResource.getK3sContainer()));
  }

  /**
   * Get external broker URL for test connectivity.
   */
  public Optional<String> getBrokerExternalUrl(KubernetesClient client)
  {
    return getBroker().map(broker -> broker.getExternalUrl(k3sResource.getK3sContainer()));
  }

  /**
   * Get external router URL for test connectivity.
   */
  public Optional<String> getRouterExternalUrl(KubernetesClient client)
  {
    return getRouter().map(router -> router.getExternalUrl(k3sResource.getK3sContainer()));
  }

  /**
   * Submits a task to the Druid cluster.
   *
   * @return the task ID
   */
  public String submitTask(String taskJson) throws Exception
  {
    Optional<String> coordinatorUrl = getCoordinatorExternalUrl();

    if (coordinatorUrl.isEmpty()) {
      throw new AssertionError("Coordinator URL not found");
    }
    String taskSubmissionUrl = coordinatorUrl.get() + "/druid/indexer/v1/task";

    HttpClient httpClient = HttpClient.newBuilder()
                                      .connectTimeout(Duration.ofSeconds(30))
                                      .build();

    HttpRequest taskRequest = HttpRequest.newBuilder()
                                         .uri(URI.create(taskSubmissionUrl))
                                         .header("Content-Type", "application/json")
                                         .header("Accept", "application/json")
                                         .POST(HttpRequest.BodyPublishers.ofString(taskJson))
                                         .timeout(Duration.ofSeconds(30))
                                         .build();

    HttpResponse<String> taskResponse = httpClient.send(taskRequest, HttpResponse.BodyHandlers.ofString());
    Assertions.assertEquals(200, taskResponse.statusCode());

    String responseBody = taskResponse.body();
    String taskId = null;
    String[] parts = responseBody.split("\"task\":\\s*\"");
    if (parts.length > 1) {
      String afterTask = parts[1];
      taskId = afterTask.split("\"")[0];
    }

    Assertions.assertNotNull(taskId, "Should be able to extract task ID from response");
    return taskId;
  }

  private void applyRBACManifests(KubernetesClient client)
  {
    try {
      client.load(new FileInputStream(Resources.getFileForResource(RBAC_MANIFEST_PATH)))
            .inNamespace(namespace)
            .createOrReplace();
    }
    catch (Exception e) {
      log.error("Error applying RBAC manifest %s: %s", RBAC_MANIFEST_PATH, e.getMessage());
      throw new RuntimeException("Failed to apply RBAC manifest: " + RBAC_MANIFEST_PATH, e);
    }
  }

  private void cleanupRBACResources(KubernetesClient client)
  {
    try {
      client.rbac().roles().inNamespace(namespace).withName("druid-cluster").delete();
      client.rbac().roleBindings().inNamespace(namespace).withName("druid-cluster").delete();

      log.info("Druid RBAC cleanup completed");
    }
    catch (Exception e) {
      log.error("Error cleaning up RBAC resources: %s", e.getMessage());
    }
  }

  /**
   * Apply individual Druid component manifests.
   */
  private void applyDruidClusterManifest(KubernetesClient client) throws Exception
  {
    log.info("Applying individual Druid component manifests...");

    for (DruidK8sComponent service : druidServices) {
      log.info("Applying manifest for Druid %s...", service.getDruidServiceType());
      service.applyDruidManifest(client);
    }
    log.info("Applied all Druid component manifests");
  }

  public void waitUntilTaskCompletes(String taskId) throws InterruptedException
  {
    HttpClient httpClient = HttpClient.newBuilder()
                                      .connectTimeout(Duration.ofSeconds(30))
                                      .build();

    String taskStatusUrl = getCoordinatorExternalUrl().get() + "/druid/indexer/v1/task/" + taskId + "/status";
    String finalStatus = "PENDING";
    int maxAttempts = 6000;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        HttpRequest statusRequest = HttpRequest.newBuilder()
                                               .uri(URI.create(taskStatusUrl))
                                               .header("Accept", "application/json")
                                               .GET()
                                               .timeout(Duration.ofSeconds(30))
                                               .build();

        HttpResponse<String> statusResponse = httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());

        if (statusResponse.statusCode() == 200) {
          String statusBody = statusResponse.body();

          if (statusBody.contains("\"status\":\"SUCCESS\"")) {
            log.debug("Task completed successfully : %s%n", statusBody);
            finalStatus = "SUCCESS";
            break;
          } else if (statusBody.contains("\"status\":\"FAILED\"")) {
            log.debug("Task failed : %s%n", statusBody);
            finalStatus = "FAILED";

          }
        } else {
          log.debug("Status check failed with HTTP %d%n", statusResponse.statusCode());
        }
        if (attempt < maxAttempts) {
          Thread.sleep(5000);
        }
      }
      catch (Exception e) {
        log.debug("Status check attempt %d failed: %s%n", attempt, e.getMessage());
        if (attempt < maxAttempts) {
          Thread.sleep(5000);
        }
      }
    }
    if (!finalStatus.equals("SUCCESS")) {
      log.error("Task %s did not complete successfully after %d attempts", taskId, maxAttempts);
      Assertions.fail("Task " + taskId + " did not complete successfully");
    }
    log.info("Final task status for task-id %s: %s", taskId, finalStatus);
  }
}