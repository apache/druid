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

package org.apache.druid.k8s.overlord;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.common.DruidKubernetesCachingClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;

import javax.annotation.Nullable;
import java.io.Closeable;

public class MultipleKubernetesTaskRunnerDelegate implements Closeable
{
  private static final Logger log = new Logger(MultipleKubernetesTaskRunnerDelegate.class);

  private final KubernetesTaskRunner runner;
  private final String k8sCluster;
  private final boolean disabled;
  private final DruidKubernetesClient kubernetesClient;
  private final DruidKubernetesCachingClient cachingClient;

  @VisibleForTesting
  MultipleKubernetesTaskRunnerDelegate(KubernetesTaskRunner runner)
  {
    this(runner, null, false, null, null);
  }

  public MultipleKubernetesTaskRunnerDelegate(KubernetesTaskRunner runner,
                                              String k8sCluster,
                                              boolean disabled,
                                              @Nullable DruidKubernetesClient kubernetesClient)
  {
    this(runner, k8sCluster, disabled, kubernetesClient, null);
  }

  public MultipleKubernetesTaskRunnerDelegate(KubernetesTaskRunner runner,
                                              String k8sCluster,
                                              boolean disabled,
                                              @Nullable DruidKubernetesClient kubernetesClient,
                                              @Nullable DruidKubernetesCachingClient cachingClient)
  {
    this.k8sCluster = k8sCluster;
    this.runner = runner;
    this.disabled = disabled;
    this.kubernetesClient = kubernetesClient;
    this.cachingClient = cachingClient;
  }

  public KubernetesTaskRunner getRunner()
  {
    return runner;
  }

  public boolean isDisabled()
  {
    return disabled;
  }

  public String getK8sCluster()
  {
    return k8sCluster;
  }

  @VisibleForTesting
  DruidKubernetesClient getKubernetesClient()
  {
    return kubernetesClient;
  }

  @Override
  public void close()
  {
    runner.stop();

    if (cachingClient != null) {
      try {
        log.info("Stopping Kubernetes caching client for cluster[%s]", k8sCluster);
        cachingClient.stop();
      }
      catch (Exception e) {
        log.warn(e, "Error while stopping Kubernetes caching client for cluster[%s]", k8sCluster);
      }
    }

    // Close the associated Kubernetes client if present
    if (kubernetesClient != null) {
      try {
        log.info("Stopping Kubernetes client for cluster[%s]", k8sCluster);
        kubernetesClient.getClient().close();
      }
      catch (Exception e) {
        log.warn(e, "Error while closing Kubernetes client for cluster[%s]", k8sCluster);
      }
    }
  }
}
