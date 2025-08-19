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

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.simulate.K3SResource;
import org.testcontainers.k3s.K3sContainer;

import java.util.concurrent.TimeUnit;

/**
 * Component for deploying the DataInfraHQ Druid Operator using Helm chart
 * in a Kubernetes test environment.
 */
public class DruidOperatorComponent implements K8sComponent
{
  private static final Logger log = new Logger(DruidOperatorComponent.class);
  
  private static final String COMPONENT_NAME = "DruidOperator";
  private static final String OPERATOR_NAMESPACE = "druid-operator-system";
  private static final String HELM_RELEASE_NAME = "druid-operator";
  private static final String HELM_REPO_NAME = "datainfra";
  private static final String HELM_REPO_URL = "https://charts.datainfra.io";
  private static final String HELM_CHART_NAME = "datainfra/druid-operator";
  private static final int POD_READY_CHECK_TIMEOUT_SECONDS = 180;

  private final String druidNamespace;
  private K3SResource k3sResource;


  public DruidOperatorComponent(String druidNamespace) 
  {
    this.druidNamespace = druidNamespace;
  }
  
  public void setK3SResource(K3SResource k3sResource) 
  {
    this.k3sResource = k3sResource;
  }

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    log.info("Initializing Helm-based Druid Operator in namespace: %s", OPERATOR_NAMESPACE);
    
    // Create namespaces
    createNamespace(client, OPERATOR_NAMESPACE);
    createNamespace(client, druidNamespace);
    
    try {
      // Add Helm repo
      log.info("Adding DataInfraHQ Helm repository...");
      executeHelmCommand("repo", "add", HELM_REPO_NAME, HELM_REPO_URL);
      
      // Update Helm repo
      log.info("Updating Helm repositories...");
      executeHelmCommand("repo", "update");
      
      // Install Druid Operator via Helm
      log.info("Installing Druid Operator via Helm...");
      executeHelmCommand(
          "install", 
          HELM_RELEASE_NAME, 
          HELM_CHART_NAME,
          "--namespace", OPERATOR_NAMESPACE,
          "--create-namespace",
          "--set", "env.WATCH_NAMESPACE=" + druidNamespace,
          "--wait",
          "--timeout", "3m"
      );
      
      log.info("✓ Helm installation completed successfully");
      
    } catch (Exception e) {
      log.error("Failed to install Druid Operator via Helm: %s", e.getMessage());
      throw new RuntimeException("Helm installation failed", e);
    }
  }

  @Override
  public void waitUntilReady(KubernetesClient client) throws Exception
  {
    try {
      log.info("Waiting for Druid Operator deployment to be ready...");
      
      // Wait for the operator deployment to be ready
      // The deployment name will be based on the Helm release name
      String deploymentName = HELM_RELEASE_NAME;
      
      client.apps().deployments()
          .inNamespace(OPERATOR_NAMESPACE)
          .withName(deploymentName)
          .waitUntilReady(POD_READY_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          
      log.info("✓ Druid Operator is ready");
      
    } catch (Exception e) {
      log.error("Timeout waiting for %s to be ready", getComponentName());
      printDiagnostics(client);
      throw e;
    }
  }

  @Override
  public void cleanup(KubernetesClient client)
  {
    try {
      log.info("Cleaning up Helm-based Druid Operator...");
      
      // Uninstall Helm release
      executeHelmCommand(
          "uninstall", 
          HELM_RELEASE_NAME,
          "--namespace", OPERATOR_NAMESPACE
      );
      
      // Remove namespace
      client.namespaces().withName(OPERATOR_NAMESPACE).delete();
      
      log.info("✓ Cleanup completed");
      
    } catch (Exception e) {
      log.error("Error during %s cleanup: %s", getComponentName(), e.getMessage());
    }
  }

  @Override
  public String getComponentName()
  {
    return COMPONENT_NAME;
  }

  @Override
  public String getNamespace()
  {
    return OPERATOR_NAMESPACE;
  }

  /**
   * Execute helm command in the K3s container
   */
  private void executeHelmCommand(String... args) throws Exception
  {
    log.info("Executing helm command: helm %s", String.join(" ", args));
    
    K3sContainer k3sContainer = k3sResource.getK3sContainer();
    
    // Build command array with environment variable and helm command
    String[] fullCommand = new String[args.length + 3];
    fullCommand[0] = "sh";
    fullCommand[1] = "-c";
    fullCommand[2] = "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml && helm " + String.join(" ", args);
    
    // Alternative approach: Build command array with "helm" as first argument
    // String[] fullCommand = new String[args.length + 1];
    // fullCommand[0] = "helm";
    // System.arraycopy(args, 0, fullCommand, 1, args.length);
    
    try {
      var result = k3sContainer.execInContainer(fullCommand);
      
      if (result.getExitCode() == 0) {
        log.info("✓ Helm command succeeded");
        if (!result.getStdout().trim().isEmpty()) {
          log.info("Output: %s", result.getStdout().trim());
        }
      } else {
        log.error("✗ Helm command failed with exit code: %d", result.getExitCode());
        if (!result.getStderr().trim().isEmpty()) {
          log.error("Error: %s", result.getStderr().trim());
        }
        throw new RuntimeException("Helm command failed: " + String.join(" ", args));
      }
    } catch (Exception e) {
      log.error("Exception executing helm command: %s", e.getMessage());
      throw e;
    }
  }

  private void createNamespace(KubernetesClient client, String namespace)
  {
    try {
      client.namespaces().resource(new NamespaceBuilder()
          .withNewMetadata()
          .withName(namespace)
          .endMetadata()
          .build()).createOrReplace();
      log.info("Created/updated namespace: %s", namespace);
    } catch (Exception e) {
      log.error("Error creating namespace %s: %s", namespace, e.getMessage());
    }
  }

  private void printDiagnostics(KubernetesClient client)
  {
    try {
      log.info("=== %s DIAGNOSTICS ===", getComponentName().toUpperCase());
      
      // Check Helm releases
      log.info("--- Helm Releases ---");
      try {
        executeHelmCommand("list", "--all-namespaces");
      } catch (Exception e) {
        log.info("Failed to list Helm releases: %s", e.getMessage());
      }
      
      log.info("--- Pod Status ---");
      client.pods().inNamespace(OPERATOR_NAMESPACE).list().getItems().forEach(pod -> {
        log.info("Pod: %s", pod.getMetadata().getName());
        log.info("  Status: %s", pod.getStatus().getPhase());
        log.info("  Ready: %s", (pod.getStatus().getConditions() != null ? 
            pod.getStatus().getConditions().stream()
                .filter(c -> "Ready".equals(c.getType()))
                .findFirst()
                .map(c -> c.getStatus())
                .orElse("Unknown") : "Unknown"));
        
        // Container statuses
        if (pod.getStatus().getContainerStatuses() != null) {
          pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
            log.info("  Container: " + containerStatus.getName());
            log.info("    Ready: " + containerStatus.getReady());
            log.info("    Restart Count: " + containerStatus.getRestartCount());
            
            if (containerStatus.getState() != null) {
              if (containerStatus.getState().getWaiting() != null) {
                log.info("    State: Waiting - " + containerStatus.getState().getWaiting().getReason());
                if (containerStatus.getState().getWaiting().getMessage() != null) {
                  log.info("    Message: " + containerStatus.getState().getWaiting().getMessage());
                }
              } else if (containerStatus.getState().getTerminated() != null) {
                log.info("    State: Terminated - " + containerStatus.getState().getTerminated().getReason());
                if (containerStatus.getState().getTerminated().getMessage() != null) {
                  log.info("    Message: " + containerStatus.getState().getTerminated().getMessage());
                }
                log.info("    Exit Code: " + containerStatus.getState().getTerminated().getExitCode());
              } else if (containerStatus.getState().getRunning() != null) {
                log.info("    State: Running since " + containerStatus.getState().getRunning().getStartedAt());
              }
            }
          });
        }
      });
      
      // Deployment status
      log.info("--- Deployment Status ---");
      var deployment = client.apps().deployments().inNamespace(OPERATOR_NAMESPACE).withName(HELM_RELEASE_NAME).get();
      if (deployment != null) {
        log.info("Deployment: " + deployment.getMetadata().getName());
        log.info("  Replicas: " + deployment.getSpec().getReplicas());
        log.info("  Ready Replicas: " + deployment.getStatus().getReadyReplicas());
        log.info("  Available Replicas: " + deployment.getStatus().getAvailableReplicas());
        
        if (deployment.getStatus().getConditions() != null) {
          deployment.getStatus().getConditions().forEach(condition -> {
            log.info("  Condition: " + condition.getType() + " = " + condition.getStatus());
            if (condition.getMessage() != null) {
              log.info("    Message: " + condition.getMessage());
            }
          });
        }
      }
      
      // Container logs
      log.info("\n--- Container Logs (last 20 lines) ---");
      client.pods().inNamespace(OPERATOR_NAMESPACE).list().getItems().forEach(pod -> {
        log.info("\n=== Logs for pod: " + pod.getMetadata().getName() + " ===");
        
        if (pod.getSpec().getContainers() != null) {
          pod.getSpec().getContainers().forEach(container -> {
            log.info("\n--- Container: " + container.getName() + " ---");
            try {
              String logs = client.pods()
                  .inNamespace(OPERATOR_NAMESPACE)
                  .withName(pod.getMetadata().getName())
                  .inContainer(container.getName())
                  .tailingLines(20)
                  .getLog();
              
              if (logs != null && !logs.trim().isEmpty()) {
                log.info(logs);
              } else {
                log.info("No logs available");
              }
            } catch (Exception e) {
              log.info("Failed to get logs: " + e.getMessage());
            }
          });
        }
      });
      
      log.info("\n=== END DIAGNOSTICS ===\n");
      
    } catch (Exception e) {
      log.error("Failed to collect diagnostics: " + e.getMessage());
    }
  }
}