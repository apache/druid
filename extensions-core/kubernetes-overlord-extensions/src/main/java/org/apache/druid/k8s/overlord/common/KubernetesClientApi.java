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

// Wraps all kubernetes api calls, to ensure you open and close connections properly
public interface KubernetesClientApi
{
  <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException;

  <T> T executePodCacheRequest(KubernetesInformerExecutor<T, Pod> executor);

  <T> T executeJobCacheRequest(KubernetesInformerExecutor<T, Job> executor);

  // use only when handling streams of data, example if you want to pass around an input stream from a pod,
  // then you would call this instead of executeRequest as you would want to keep the connection open until you
  // are done with the stream.  Callers responsibility to clean up when using this method
  KubernetesClient getClient();

  SharedIndexInformer<Pod> getPodInformer();

  SharedIndexInformer<Job> getJobInformer();

  long getInformerResyncPeriodMillis();

  KubernetesResourceEventNotifier getEventNotifier();
}
