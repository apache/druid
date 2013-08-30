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

package com.metamx.druid.client.indexing;

import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.client.indexing.IndexingService;
import io.druid.client.selector.DiscoverySelector;
import io.druid.client.selector.Server;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import javax.annotation.Nullable;
import java.io.IOException;

/**
*/
public class IndexingServiceSelector implements DiscoverySelector<Server>
{
  private static final Logger log = new Logger(IndexingServiceSelector.class);

  private final ServiceProvider serviceProvider;

  @Inject
  public IndexingServiceSelector(
      @Nullable @IndexingService ServiceProvider serviceProvider
  ) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public Server pick()
  {
    final ServiceInstance instance;
    try {
      instance = serviceProvider.getInstance();
    }
    catch (Exception e) {
      log.info(e, "");
      return null;
    }

    return new Server()
    {
      @Override
      public String getHost()
      {
        return instance.getAddress();
      }

      @Override
      public int getPort()
      {
        return instance.getPort();
      }

      @Override
      public String getScheme()
      {
        return "http";
      }
    };
  }

  @LifecycleStart
  public void start() throws Exception
  {
    serviceProvider.start();
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    serviceProvider.close();
  }
}
