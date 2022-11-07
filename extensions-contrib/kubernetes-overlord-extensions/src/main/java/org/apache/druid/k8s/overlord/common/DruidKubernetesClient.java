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

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class DruidKubernetesClient implements KubernetesClientApi
{

  private final Config config;

  public DruidKubernetesClient()
  {
    this(Config.autoConfigure(null));
  }

  public DruidKubernetesClient(Config config)
  {
    this.config = config;
  }

  @Override
  public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
  {
    try (KubernetesClient client = new DefaultKubernetesClient(config)) {
      return executor.executeRequest(client);
    }
  }
}
