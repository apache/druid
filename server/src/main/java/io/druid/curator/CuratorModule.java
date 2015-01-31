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

package io.druid.curator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.guice.ConfigProvider;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;

import java.io.IOException;

/**
 */
public class CuratorModule implements Module
{
  private static final Logger log = new Logger(CuratorModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(
        binder, "druid.zk.service",
        CuratorConfig.class);
  }

  @Provides @LazySingleton
  public CuratorFramework makeCurator(CuratorConfig config, Lifecycle lifecycle) throws IOException
  {
    final CuratorFramework framework =
        CuratorFrameworkFactory.builder()
                               .connectString(config.getZkHosts())
                               .sessionTimeoutMs(config.getZkSessionTimeoutMs())
            .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
            .compressionProvider(new PotentiallyGzippedCompressionProvider(config.getEnableCompression()))
            .build();

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            log.info("Starting Curator");
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
}
