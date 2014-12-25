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

package io.druid.testing.utils;

import com.metamx.common.logger.Logger;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;

import java.util.concurrent.Callable;

public class ServerDiscoveryUtil
{

  private static final Logger LOG = new Logger(ServerDiscoveryUtil.class);

  public static boolean isInstanceReady(ServerDiscoverySelector serviceProvider)
  {
    try {
      Server instance = serviceProvider.pick();
      if (instance == null) {
        LOG.warn("Unable to find a host");
        return false;
      }
    }
    catch (Exception e) {
      LOG.error(e, "Caught exception waiting for host");
      return false;
    }
    return true;
  }

  public static void waitUntilInstanceReady(final ServerDiscoverySelector serviceProvider, String instanceType)
  {
    RetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return isInstanceReady(serviceProvider);
          }
        }, String.format("Instance %s to get ready", instanceType)
    );
  }

}
