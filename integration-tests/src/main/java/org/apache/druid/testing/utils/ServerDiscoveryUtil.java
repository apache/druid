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

package org.apache.druid.testing.utils;

import org.apache.druid.client.selector.Server;
import org.apache.druid.curator.discovery.ServerDiscoverySelector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

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
    ITRetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call()
          {
            return isInstanceReady(serviceProvider);
          }
        },
        StringUtils.format("Instance %s to get ready", instanceType)
    );
  }

}
