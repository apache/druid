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

package org.apache.druid.testsEx.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testsEx.config.ResolvedConfig;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedZk;

import java.util.concurrent.TimeUnit;

/**
 * Test oriented ZooKeeper client.
 * <p>
 * Currently contains just enough functionality to verify that
 * ZK is ready.
 */
public class ZooKeeperClient
{
  private final ResolvedConfig clusterConfig;
  private final ResolvedZk config;
  private CuratorFramework curatorFramework;

  public ZooKeeperClient(ResolvedConfig config)
  {
    this.clusterConfig = config;
    this.config = config.zk();
    if (this.config == null) {
      throw new ISE("ZooKeeper not configured");
    }
    prepare();
    awaitReady();
  }

  private void prepare()
  {
    CuratorConfig curatorConfig = clusterConfig.toCuratorConfig();
    curatorFramework = CuratorModule.createCurator(curatorConfig);
  }

  private void awaitReady()
  {
    int timeoutSec = config.startTimeoutSecs();
    if (timeoutSec == 0) {
      timeoutSec = 5;
    }
    try {
      curatorFramework.start();
      curatorFramework.blockUntilConnected(timeoutSec, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      throw new ISE("ZooKeeper timed out waiting for connect");
    }
  }

  public CuratorFramework curator()
  {
    return curatorFramework;
  }

  public void close()
  {
    curatorFramework.close();
    curatorFramework = null;
  }
}
