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
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Kubernetes-based tests. Provides a K3s cluster
 * and manages K8s components lifecycle.
 */
public abstract class KubernetesTestBase
{
  private static final Logger log = new Logger(KubernetesTestBase.class);
  
  private static final K3SResource k3sContainer = new K3SResource();
  private static final MinIOStorageResource minIOStorage = new MinIOStorageResource();
  private static KubernetesClient client;
  private static List<ComponentEntry> components = new ArrayList<>();

  /**
   * Internal class to track component state
   */
  private static class ComponentEntry
  {
    private final K8sComponent component;
    private boolean initialized;
    private final boolean cleanupInEachTest;

    public ComponentEntry(K8sComponent component, boolean cleanupInEachTest)
    {
      this.component = component;
      this.initialized = false;
      this.cleanupInEachTest = cleanupInEachTest;
    }

    public K8sComponent getComponent()
    {
      return component;
    }

    public boolean isInitialized()
    {
      return initialized;
    }

    public void setInitialized(boolean initialized)
    {
      this.initialized = initialized;
    }

    public boolean shouldCleanupInEachTest()
    {
      return cleanupInEachTest;
    }
  }

  protected static void startK3SContainer()
  {
    log.info("Starting K3s conatiner...");
    k3sContainer.start();
    client = k3sContainer.getClient();
  }

  protected static void stopK3SContainer()
  {
    log.info("Stopping K3s cluster...");
    k3sContainer.stop();
  }

  protected KubernetesClient getClient()
  {
    return client;
  }

  protected static K3SResource getDeployingResource()
  {
    return k3sContainer;
  }

  /**
   * Add a kubernetes component to be deployed in k3s container.
   * By default, components are cleaned up in each test.
   *
   * @param component the component to add
   */
  protected static void addKubernetesComponent(K8sComponent component)
  {
    components.add(new ComponentEntry(component, true));
  }

  /**
   * Add a kubernetes component to be deployed in k3s container.
   *
   * @param component the component to add
   * @param cleanupInEachTest whether this component should be cleaned up in each test
   */
  protected static void addKubernetesComponent(K8sComponent component, boolean cleanupInEachTest)
  {
    components.add(new ComponentEntry(component, cleanupInEachTest));
  }

  /**
   * Initialize all registered components in order.
   */
  protected static void initializeComponents()
  {
    for (ComponentEntry entry : components) {
      if (entry.isInitialized()) {
        log.info("Skipping already initialized component: %s", entry.getComponent().getComponentName());
        continue;
      }
      final K8sComponent component = entry.getComponent();
      try {
        component.initialize(client);
        component.waitUntilReady(client);
        entry.setInitialized(true);
      } catch (Exception e) {
        log.error("Failed to initialize %s: %s", component.getComponentName(), e.getMessage());
        throw new RuntimeException("Component initialization failed", e);
      }
    }
  }

  protected static void cleanupComponents(boolean skipStatic)
  {
    log.info("Starting cleanup with skipStatic=%s, component count=%d", skipStatic, components.size());
    
    for (int i = components.size() - 1; i >= 0; i--) {
      ComponentEntry entry = components.get(i);
      
      log.info("Checking component %s: initialized=%s, cleanupInEachTest=%s", 
          entry.getComponent().getComponentName(), entry.isInitialized(), entry.shouldCleanupInEachTest());
      
      if (!entry.isInitialized()) {
        log.info("Skipping uninitialized component: %s", entry.getComponent().getComponentName());
        continue;
      }
      
      if (!entry.shouldCleanupInEachTest() && !skipStatic) {
        log.info("Skipping static component: %s", entry.getComponent().getComponentName());
        continue;
      }
      
      K8sComponent component = entry.getComponent();
      try {
        log.info("Cleaning up component: %s", component.getComponentName());
        component.cleanup(client);
        components.remove(i);
        log.info("Successfully cleaned up and removed: %s", component.getComponentName());
      } catch (Exception e) {
        log.error("Error cleaning up %s: %s", component.getComponentName(), e.getMessage());
      }
    }
    
    log.info("Cleanup completed, remaining component count=%d", components.size());
  }

  protected static void createNamespace(String namespace)
  {
    try {
      client.namespaces().resource(new NamespaceBuilder()
          .withNewMetadata()
          .withName(namespace)
          .endMetadata()
          .build()).create();
    } catch (Exception e) {
      log.error("Error creating namespace %s: %s", namespace, e.getMessage());
      throw new RuntimeException("Failed to create namespace", e);
    }
  }
}