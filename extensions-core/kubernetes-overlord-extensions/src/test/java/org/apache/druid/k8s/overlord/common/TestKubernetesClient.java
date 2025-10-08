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

package org.apache.druid.k8s.overlord.common;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

public class TestKubernetesClient implements KubernetesClientApi
{

  private final KubernetesClient client;
  private final SharedIndexInformer<Pod> podInformer;
  private final SharedIndexInformer<Job> jobInformer;

  public TestKubernetesClient(KubernetesClient client,
                              SharedIndexInformer<Pod> podInformer,
                              SharedIndexInformer<Job> jobInformer
  )
  {
    this.client = client;
    this.podInformer = podInformer;
    this.jobInformer = jobInformer;
  }

  @Override
  public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
  {
    return executor.executeRequest(client);
  }

  @Override
  public <T> T executePodCacheRequest(KubernetesInformerExecutor<T, Pod> executor)
  {
    return executor.executeRequest(podInformer);
  }

  @Override
  public <T> T executeJobCacheRequest(KubernetesInformerExecutor<T, Job> executor)
  {
    return executor.executeRequest(jobInformer);
  }

  @Override
  public KubernetesClient getClient()
  {
    return client;
  }

  @Override
  public SharedIndexInformer<Pod> getPodInformer()
  {
    return podInformer;
  }

  @Override
  public SharedIndexInformer<Job> getJobInformer()
  {
    return jobInformer;
  }

  @Override
  public KubernetesResourceEventNotifier getEventNotifier()
  {
    return null;
  }
}
