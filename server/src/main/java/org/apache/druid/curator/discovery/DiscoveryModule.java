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

package org.apache.druid.curator.discovery;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ServiceAnnouncementState;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.util.function.Function;

/**
 * Binds {@link DruidNodeAnnouncer}, {@link DruidNodeDiscoveryProvider}, and the coordinator/overlord
 * {@link DruidLeaderSelector}s to their curator-backed implementations.
 */
public class DiscoveryModule implements Module
{
  private static final String INTERNAL_DISCOVERY_PROP = "druid.discovery.type";
  private static final String CURATOR_KEY = "curator";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(ServiceAnnouncementState.class).in(LazySingleton.class);

    PolyBind.createChoiceWithDefault(binder, INTERNAL_DISCOVERY_PROP, Key.get(DruidNodeAnnouncer.class), CURATOR_KEY);

    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidNodeDiscoveryProvider.class),
        CURATOR_KEY
    );

    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidLeaderSelector.class, Coordinator.class),
        CURATOR_KEY
    );

    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidLeaderSelector.class, IndexingService.class),
        CURATOR_KEY
    );

    PolyBind.optionBinder(binder, Key.get(DruidNodeDiscoveryProvider.class))
            .addBinding(CURATOR_KEY)
            .to(CuratorDruidNodeDiscoveryProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeAnnouncer.class))
            .addBinding(CURATOR_KEY)
            .to(CuratorDruidNodeAnnouncer.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, Coordinator.class))
            .addBinding(CURATOR_KEY)
            .toProvider(
                new DruidLeaderSelectorProvider(
                    zkPathsConfig -> ZKPaths.makePath(zkPathsConfig.getCoordinatorPath(), "_COORDINATOR")
                )
            )
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, IndexingService.class))
            .addBinding(CURATOR_KEY)
            .toProvider(
                new DruidLeaderSelectorProvider(
                    zkPathsConfig -> ZKPaths.makePath(zkPathsConfig.getOverlordPath(), "_OVERLORD")
                )
            )
            .in(LazySingleton.class);
  }

  private static class DruidLeaderSelectorProvider implements Provider<DruidLeaderSelector>
  {
    @Inject
    private Provider<CuratorFramework> curatorFramework;

    @Inject
    @Self
    private DruidNode druidNode;

    @Inject
    private ZkPathsConfig zkPathsConfig;

    private final Function<ZkPathsConfig, String> latchPathFn;

    DruidLeaderSelectorProvider(Function<ZkPathsConfig, String> latchPathFn)
    {
      this.latchPathFn = latchPathFn;
    }

    @Override
    public DruidLeaderSelector get()
    {
      return new CuratorDruidLeaderSelector(
          curatorFramework.get(),
          druidNode,
          latchPathFn.apply(zkPathsConfig)
      );
    }
  }
}
