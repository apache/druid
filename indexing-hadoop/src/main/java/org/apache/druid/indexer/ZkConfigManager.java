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

package org.apache.druid.indexer;

import com.google.inject.Inject;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.ZkPathsConfig;

@ManageLifecycle
public class ZkConfigManager
{

  private static final Logger log = new Logger(ZkConfigManager.class);


  private static volatile String host;

  private static volatile String base;


  @Inject
  public ZkConfigManager(
      CuratorConfig curatorConfig,
      ZkPathsConfig zkPathsConfig
  )
  {
    synchronized (ZkConfigManager.class) {
      host = curatorConfig.getZkHosts();
      base = zkPathsConfig.getBase();
      log.info("ZkConfigManager get host:%s, base:%s", host, base);
    }

  }


  public static String getZkHosts()
  {
    synchronized (ZkConfigManager.class) {
      return host;
    }
  }

  public static String getBase()
  {
    synchronized (ZkConfigManager.class) {
      return base;
    }
  }
}
