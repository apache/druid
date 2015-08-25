/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Command;
import io.druid.concurrent.Execs;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.announcement.Announcer;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.MetadataSegmentManagerConfig;
import io.druid.metadata.MetadataSegmentManagerProvider;
import io.druid.guice.ConfigProvider;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.ManageLifecycleLast;
import io.druid.guice.NodeTypeConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.QueryResource;
import io.druid.server.bridge.Bridge;
import io.druid.server.bridge.BridgeCuratorConfig;
import io.druid.server.bridge.BridgeQuerySegmentWalker;
import io.druid.server.bridge.BridgeZkCoordinator;
import io.druid.server.bridge.DruidClusterBridge;
import io.druid.server.bridge.DruidClusterBridgeConfig;
import io.druid.server.coordination.AbstractDataSegmentAnnouncer;
import io.druid.server.coordination.BatchDataSegmentAnnouncer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.initialization.jetty.JettyServerInitializer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "bridge",
    description = "This is a highly experimental node to use at your own discretion"
)
public class CliBridge extends ServerRunnable
{
  private static final Logger log = new Logger(CliBridge.class);

  public CliBridge()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/bridge");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8089);

            ConfigProvider.bind(binder, BridgeCuratorConfig.class);

            binder.bind(BridgeZkCoordinator.class).in(ManageLifecycle.class);
            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig("bridge"));

            JsonConfigProvider.bind(binder, "druid.manager.segments", MetadataSegmentManagerConfig.class);
            binder.bind(MetadataSegmentManager.class)
                  .toProvider(MetadataSegmentManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(QuerySegmentWalker.class).to(BridgeQuerySegmentWalker.class).in(LazySingleton.class);
            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            Jerseys.addResource(binder, QueryResource.class);
            LifecycleModule.register(binder, QueryResource.class);

            ConfigProvider.bind(binder, DruidClusterBridgeConfig.class);
            binder.bind(DruidClusterBridge.class);
            LifecycleModule.register(binder, DruidClusterBridge.class);

            LifecycleModule.register(binder, BridgeZkCoordinator.class);

            LifecycleModule.register(binder, Server.class);
          }

          @Provides
          @LazySingleton
          @Bridge
          public CuratorFramework getBridgeCurator(final BridgeCuratorConfig bridgeCuratorConfig, Lifecycle lifecycle)
          {
            final CuratorFramework framework =
                CuratorFrameworkFactory.builder()
                                       .connectString(bridgeCuratorConfig.getParentZkHosts())
                                       .sessionTimeoutMs(bridgeCuratorConfig.getZkSessionTimeoutMs())
                                       .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
                                       .compressionProvider(
                                           new PotentiallyGzippedCompressionProvider(
                                               bridgeCuratorConfig.getEnableCompression()
                                           )
                                       )
                                       .build();

            lifecycle.addHandler(
                new Lifecycle.Handler()
                {
                  @Override
                  public void start() throws Exception
                  {
                    log.info("Starting Curator for %s", bridgeCuratorConfig.getParentZkHosts());
                    framework.start();
                  }

                  @Override
                  public void stop()
                  {
                    log.info("Stopping Curator");
                    framework.close();
                  }
                }
            );

            return framework;
          }

          @Provides
          @ManageLifecycle
          public ServerDiscoverySelector getServerDiscoverySelector(
              DruidClusterBridgeConfig config,
              ServerDiscoveryFactory factory

          )
          {
            return factory.createSelector(config.getBrokerServiceName());
          }

          @Provides
          @ManageLifecycle
          @Bridge
          public Announcer getBridgeAnnouncer(
              @Bridge CuratorFramework curator
          )
          {
            return new Announcer(curator, Execs.singleThreaded("BridgeAnnouncer-%s"));
          }

          @Provides
          @ManageLifecycleLast
          @Bridge
          public AbstractDataSegmentAnnouncer getBridgeDataSegmentAnnouncer(
              DruidServerMetadata metadata,
              BatchDataSegmentAnnouncerConfig config,
              ZkPathsConfig zkPathsConfig,
              @Bridge Announcer announcer,
              ObjectMapper jsonMapper
          )
          {
            return new BatchDataSegmentAnnouncer(
                metadata,
                config,
                zkPathsConfig,
                announcer,
                jsonMapper
            );
          }
        }
    );
  }
}
