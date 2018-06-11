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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.java.util.http.client.HttpClient;
import io.druid.client.indexing.IndexingService;
import io.druid.client.indexing.IndexingServiceSelectorConfig;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.discovery.DruidLeaderClient;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.annotations.EscalatedGlobal;

/**
 */
public class IndexingServiceDiscoveryModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.selectors.indexing", IndexingServiceSelectorConfig.class);
  }

  @Provides
  @IndexingService
  @ManageLifecycle
  public ServerDiscoverySelector getServiceProvider(
      IndexingServiceSelectorConfig config,
      ServerDiscoveryFactory serverDiscoveryFactory
  )
  {
    return serverDiscoveryFactory.createSelector(config.getServiceName());
  }

  @Provides
  @IndexingService
  @ManageLifecycle
  public DruidLeaderClient getLeaderHttpClient(
      @EscalatedGlobal HttpClient httpClient,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      @IndexingService ServerDiscoverySelector serverDiscoverySelector
  )
  {
    return new DruidLeaderClient(
        httpClient,
        druidNodeDiscoveryProvider,
        DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
        "/druid/indexer/v1/leader",
        serverDiscoverySelector
    );
  }
}
