/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.discovery;

import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServerDiscoverySelector;

import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidLeaderClientProvider
{
  private final AtomicReference<DruidLeaderClient> instanceRef = new AtomicReference<>();

  private final HttpClient httpClient;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final String nodeTypeToWatch;

  private final String leaderRequestPath;

  private final ServerDiscoverySelector serverDiscoverySelector;

  public DruidLeaderClientProvider(
      HttpClient httpClient,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      String nodeTypeToWatch,
      String leaderRequestPath,
      ServerDiscoverySelector serverDiscoverySelector
  )
  {
    this.httpClient = httpClient;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.nodeTypeToWatch = nodeTypeToWatch;
    this.leaderRequestPath = leaderRequestPath;
    this.serverDiscoverySelector = serverDiscoverySelector;
  }

  public DruidLeaderClient get()
  {
    return instanceRef.accumulateAndGet(
        null,
        (current, given) -> {
          if (current != null) {
            return current;
          }

          return new DruidLeaderClient(
              httpClient,
              druidNodeDiscoveryProvider.getForNodeType(nodeTypeToWatch),
              leaderRequestPath,
              serverDiscoverySelector
          );
        }
    );
  }
}
