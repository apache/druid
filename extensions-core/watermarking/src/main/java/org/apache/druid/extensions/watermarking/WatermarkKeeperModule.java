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
import org.apache.druid.extensions.watermarking.http.WatermarkKeeperResource;
import org.apache.druid.extensions.watermarking.watermarks.WatermarkCursorFactory;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WatermarkKeeperModule implements Module
{
  private static final Logger log = new Logger(WatermarkKeeperModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
        WatermarkKeeper.SERVICE_NAME
    );
    binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
    binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
    JsonConfigProvider.bind(binder, "druid.watermarking.keeper", WatermarkKeeperConfig.class);
    binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
    Jerseys.addResource(binder, WatermarkKeeperResource.class);

    LifecycleModule.register(binder, WatermarkKeeper.class);
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
}
