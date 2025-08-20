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

    createNamespace(client, OPERATOR_NAMESPACE);
    createNamespace(client, druidNamespace);

    try {
      log.info("Adding DataInfraHQ Helm repository...");
      executeHelmCommand("repo", "add", HELM_REPO_NAME, HELM_REPO_URL);

      log.info("Updating Helm repositories...");
      executeHelmCommand("repo", "update");

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

    }
    catch (Exception e) {
      log.error("Failed to install Druid Operator via Helm: %s", e.getMessage());
      throw new RuntimeException("Helm installation failed", e);
    }
  }

  @Override
  public void waitUntilReady(KubernetesClient client) throws Exception
  {
    try {
      log.info("Waiting for Druid Operator deployment to be ready...");

      String deploymentName = HELM_RELEASE_NAME;

      client.apps().deployments()
            .inNamespace(OPERATOR_NAMESPACE)
            .withName(deploymentName)
            .waitUntilReady(POD_READY_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      log.info("✓ Druid Operator is ready");

    }
    catch (Exception e) {
      log.error("Timeout waiting for %s to be ready", getComponentName());
      throw e;
    }
  }

  @Override
  public void cleanup(KubernetesClient client)
  {
    try {
      log.info("Cleaning up Helm-based Druid Operator...");

      executeHelmCommand(
          "uninstall",
          HELM_RELEASE_NAME,
          "--namespace", OPERATOR_NAMESPACE
      );

      client.namespaces().withName(OPERATOR_NAMESPACE).delete();

      log.info("✓ Cleanup completed");

    }
    catch (Exception e) {
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

    String[] fullCommand = new String[args.length + 3];
    fullCommand[0] = "sh";
    fullCommand[1] = "-c";
    fullCommand[2] = "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml && helm " + String.join(" ", args);

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
    }
    catch (Exception e) {
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
    }
    catch (Exception e) {
      log.error("Error creating namespace %s: %s", namespace, e.getMessage());
    }
  }
}
