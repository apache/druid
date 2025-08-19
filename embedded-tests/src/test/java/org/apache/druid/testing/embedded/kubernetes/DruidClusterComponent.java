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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.simulate.K3SResource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.utils.PortAllocator;
import org.testcontainers.k3s.K3sContainer;

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
    
    druidServices.add(service);
    log.info("Added Druid service %s with allocated NodePort %d", 
        service.getDruidServiceType(), allocatedPort);
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
    
    // Give all services 2 seconds to start up and begin service discovery
    log.info("Allowing 2 seconds for all Druid services to start and discover each other...");
    Thread.sleep(2000);
    
    // Now wait for each service to be ready (blocking calls, but after initial delay)
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
      } catch (Exception e) {
        log.error("Error cleaning up %s: %s", service.getDruidServiceType(), e.getMessage());
      }
    }
    
    // Clean up RBAC resources
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

  public String getClusterName()
  {
    return clusterName;
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
        .collect(java.util.stream.Collectors.toList());
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

  public Optional<String> getCoordinatorUrl()
  {
    return getCoordinator().map(DruidK8sComponent::getServiceUrl);
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
  public Optional<String> getCoordinatorExternalUrl(KubernetesClient client)
  {
    return getCoordinator().map(coordinator -> coordinator.getExternalUrl(client, k3sResource.getK3sContainer()));
  }

  /**
   * Get external broker URL for test connectivity.
   */
  public Optional<String> getBrokerExternalUrl(KubernetesClient client)
  {
    return getBroker().map(broker -> broker.getExternalUrl(client, k3sResource.getK3sContainer()));
  }

  /**
   * Get external router URL for test connectivity.
   */
  public Optional<String> getRouterExternalUrl(KubernetesClient client)
  {
    return getRouter().map(router -> router.getExternalUrl(client, k3sResource.getK3sContainer()));
  }

  /**
   * Submits a task to the Druid cluster and waits for it to complete successfully.
   */
  public boolean submitTaskAndWait(Task task, int timeoutSeconds, KubernetesClient client) throws Exception
  {
    String taskId = submitTask(task, client);
    return waitForTaskToComplete(taskId, timeoutSeconds, client);
  }

  /**
   * Submits a task to the Druid cluster.
   * 
   * @param task the task to submit
   * @param client the Kubernetes client for external URL access
   * @return the task ID
   * @throws Exception if task submission fails
   */
  public String submitTask(Task task, KubernetesClient client) throws Exception
  {
    Optional<String> coordinatorUrl = getCoordinatorExternalUrl(client);
    if (coordinatorUrl.isEmpty()) {
      throw new IllegalStateException("Coordinator external URL not available");
    }

    log.info("Using coordinator URL: %s", coordinatorUrl.get());

    String taskJson = TestHelper.JSON_MAPPER.writeValueAsString(task);
    String taskSubmissionUrl = coordinatorUrl.get() + "/druid/indexer/v1/task";
    
    // Retry task submission with fresh HttpClient for each attempt
    HttpResponse<String> response = null;
    Exception lastException = null;
    
    for (int attempt = 1; attempt <= 5; attempt++) {
      try {
        HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
            
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(taskSubmissionUrl))
            .header("Content-Type", "application/json")
            .header("User-Agent", "DruidTest/1.0")
            .POST(HttpRequest.BodyPublishers.ofString(taskJson))
            .timeout(Duration.ofSeconds(60))
            .build();

        response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response == null) {
          throw new RuntimeException("Task submission failed after 3 attempts", lastException);
        }
        if (response.statusCode() != 200) {
          log.error("Task submission failed on attempt %d. Status: %d, Response: %s", attempt, response.statusCode(), response.body());
//          throw new RuntimeException("Task submission failed. Status: " + response.statusCode()
//                                     + ", Response: " + response.body());
        } else {
          log.info("Task %s submitted successfully on attempt %d", task.getId(), attempt);
          break; // Exit loop on success
        }


      } catch (Exception e) {
        lastException = e;
        log.warn("Task submission attempt %d failed: %s", attempt, e.getMessage());
        if (attempt < 3) {
          Thread.sleep(2000); // Wait 2 seconds before retry
        }
      }

    }

    log.info("Task %s submitted successfully", task.getId());
    return task.getId();
  }

  /**
   * Waits for a task to complete successfully.
   */
  public boolean waitForTaskToComplete(String taskId, int timeoutSeconds, KubernetesClient client) throws Exception
  {
    Optional<String> coordinatorUrl = getCoordinatorExternalUrl(client);
    if (coordinatorUrl.isEmpty()) {
      throw new IllegalStateException("Coordinator external URL not available");
    }

    String taskStatusUrl = coordinatorUrl.get() + "/druid/indexer/v1/task/" + taskId + "/status";
    
    boolean taskCompleted = false;
    int waitedSeconds = 0;
    
    log.info("Waiting for task %s to complete (timeout: %d seconds)...", taskId, timeoutSeconds);
    
    while (!taskCompleted && waitedSeconds < timeoutSeconds) {
      // Create fresh HttpClient for each status check
      HttpClient httpClient = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(30))
          .build();
          
      HttpRequest statusRequest = HttpRequest.newBuilder()
          .uri(URI.create(taskStatusUrl))
          .header("User-Agent", "DruidTest/1.0")
          .GET()
          .timeout(Duration.ofSeconds(10))
          .build();

      HttpResponse<String> statusResponse = null;
      
      try {
        statusResponse = httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
      } catch (Exception e) {
        log.warn("Error checking task status (attempt will retry): %s", e.getMessage());
        Thread.sleep(5000);
        waitedSeconds += 5;
        continue;
      }
      
      if (statusResponse.statusCode() == 200) {
        String responseBody = statusResponse.body();
        if (responseBody.contains("\"status\":\"SUCCESS\"")) {
          log.info("Task %s completed successfully", taskId);
          taskCompleted = true;
          break;
        } else if (responseBody.contains("\"status\":\"FAILED\"")) {
          throw new RuntimeException("Task " + taskId + " failed: " + responseBody);
        }
        
        // Log progress every 30 seconds
        if (waitedSeconds % 30 == 0) {
          log.info("Task %s still running after %d seconds...", taskId, waitedSeconds);
        }
      } else {
        log.warn("Failed to get task status (HTTP %d): %s", 
            statusResponse.statusCode(), statusResponse.body());
      }
      
      Thread.sleep(5000);
      waitedSeconds += 5;
    }
    
    if (!taskCompleted) {
      log.warn("Task %s did not complete within %d seconds", taskId, timeoutSeconds);
    }
    
    return taskCompleted;
  }

  private void applyRBACManifests(KubernetesClient client)
  {
    try {
      client.load(new FileInputStream(Resources.getFileForResource(RBAC_MANIFEST_PATH)))
          .inNamespace(namespace)
          .createOrReplace();
    } catch (Exception e) {
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
    } catch (Exception e) {
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
  
}