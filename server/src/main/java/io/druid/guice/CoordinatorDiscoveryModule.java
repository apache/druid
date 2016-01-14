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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.client.DruidServerDiscovery;
import io.druid.client.coordinator.Coordinator;
import io.druid.client.coordinator.CoordinatorSelectorConfig;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.server.coordination.DruidServerMetadata;

import java.util.List;

/**
 */
public class CoordinatorDiscoveryModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
  }

  @Provides
  @Coordinator
  public ServerDiscoverySelector getServiceProvider(
      CoordinatorSelectorConfig config,
      ServerDiscoveryFactory serverDiscoveryFactory
  )
  {
    return serverDiscoveryFactory.createSelector(new Function<DruidServerDiscovery, List<DruidServerMetadata>>()
    {
      @Override
      public List<DruidServerMetadata> apply(DruidServerDiscovery discovery)
      {
        final DruidServerMetadata leader;
        try {
          leader = discovery.getLeaderForType("coordinator");
          return leader == null ? null : Lists.newArrayList(leader);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }
}
