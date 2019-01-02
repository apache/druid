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

package org.apache.druid.extensions.watermarking;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import org.apache.druid.cli.QueryJettyServerInitializer;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollectorServerView;
import org.apache.druid.extensions.watermarking.gaps.GapDetectorFactory;
import org.apache.druid.extensions.watermarking.http.WatermarkCollectorResource;
import org.apache.druid.extensions.watermarking.http.WatermarkKeeperResource;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCacheConfig;
import org.apache.druid.extensions.watermarking.watermarks.WatermarkCursorFactory;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WatermarkCollectorModule implements Module
{
  private static final Logger log = new Logger(WatermarkCollectorModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
        WatermarkCollector.SERVICE_NAME
    );
    binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
    binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
    JsonConfigProvider.bind(binder, "druid.watermarking.collector", WatermarkCollectorConfig.class);

    binder.bind(CachingClusteredClient.class).in(LazySingleton.class);

    binder.bind(TimelineMetadataCollectorServerView.class).in(LazySingleton.class);
    binder.bind(TimelineServerView.class).to(TimelineMetadataCollectorServerView.class).in(LazySingleton.class);
    binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
    binder.install(new CacheModule());
    JsonConfigProvider.bind(binder, "druid.watermarking.cache", WatermarkCacheConfig.class);

    JsonConfigProvider.bind(binder, "druid.broker.select", TierSelectorStrategy.class);
    JsonConfigProvider.bind(binder, "druid.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
    JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);
    JsonConfigProvider.bind(binder, "druid.broker.retryPolicy", RetryQueryRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.broker.segment", BrokerSegmentWatcherConfig.class);

    binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

    binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

    Jerseys.addResource(binder, WatermarkKeeperResource.class);
    Jerseys.addResource(binder, WatermarkCollectorResource.class);

    LifecycleModule.register(binder, WatermarkCollector.class);
    LifecycleModule.register(binder, Server.class);
  }

  @Provides
  @LazySingleton
  public List<WatermarkCursorFactory> getWatermarkCursorBuilders(
      WatermarkCollectorConfig collectorConfig,
      Map<String, WatermarkCursorFactory> factories,
      Injector injector
  )
  {
    List<WatermarkCursorFactory> cursorFactories = new ArrayList<>();

    List<Class<? extends WatermarkCursorFactory>> configCursorFactories = collectorConfig.getCursors();
    if (configCursorFactories != null && configCursorFactories.size() > 0) {
      for (Class<? extends WatermarkCursorFactory> cursorFactoryClass : collectorConfig.getCursors()) {
        final WatermarkCursorFactory cursorBuilder = injector.getInstance(cursorFactoryClass);

        log.info("Adding watermark cursor factory [%s]", cursorBuilder);

        cursorFactories.add(cursorBuilder);
      }
    } else {
      for (WatermarkCursorFactory factory : factories.values()) {
        log.info("Adding watermark cursor factory [%s]", factory);

        cursorFactories.add(factory);
      }
    }

    return cursorFactories;
  }

  @Provides
  @LazySingleton
  public List<GapDetectorFactory> getGapDetectorFactories(
      WatermarkCollectorConfig collectorConfig,
      Map<String, GapDetectorFactory> factories,
      Injector injector
  )
  {
    List<GapDetectorFactory> gapDetectorFactories = new ArrayList<>();

    List<Class<? extends GapDetectorFactory>> configGapDetectors = collectorConfig.getGapDetectors();
    if (configGapDetectors != null && configGapDetectors.size() > 0) {
      for (Class<? extends GapDetectorFactory> gapDetectorFactoryClass : collectorConfig.getGapDetectors()) {
        final GapDetectorFactory cursorBuilder = injector.getInstance(gapDetectorFactoryClass);

        log.info("Adding gap detector factory [%s]", cursorBuilder);

        gapDetectorFactories.add(cursorBuilder);
      }
    } else {
      for (GapDetectorFactory factory : factories.values()) {
        log.info("Adding gap detector factory [%s]", factory);

        gapDetectorFactories.add(factory);
      }
    }

    return gapDetectorFactories;
  }
}
