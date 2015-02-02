/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.curator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
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
