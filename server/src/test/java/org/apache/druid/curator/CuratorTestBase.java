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

package org.apache.druid.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class CuratorTestBase
{
  protected TestingServer server;
  protected Timing timing;
  protected CuratorFramework curator;

  public void setupServerAndCurator() throws Exception
  {
    server = new TestingServer();
    timing = new Timing();
    curator = createCurator();
  }

  private CuratorFramework createCurator()
  {
    return CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(timing.session())
        .connectionTimeoutMs(timing.connection())
        .retryPolicy(new RetryOneTime(1))
        .compressionProvider(new PotentiallyGzippedCompressionProvider(true))
        .build();
  }

  public void tearDownServerAndCurator()
  {
    try {
      curator.close();
      server.close();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public String getConnectString()
  {
    return server.getConnectString();
  }

  /**
   * Starts a new client, with its own session, that owns an ephemeral node at the given path.
   */
  public CuratorFramework createEphemeralNodeInNewSession(final String path, final byte[] bytes) throws Exception
  {
    final CuratorFramework client = createCurator();
    try {
      client.start();
      client.blockUntilConnected();
      client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
      return client;
    }
    catch (Exception e) {
      client.close();
      throw e;
    }
  }

  /**
   * Waits until the node at the given path holds the given bytes and belongs to the session of {@link #curator}.
   */
  public void awaitAnnounced(final String path, final byte[] bytes) throws Exception
  {
    final long deadline = System.currentTimeMillis() + timing.forWaiting().milliseconds();
    while (!isAnnounced(path, bytes)) {
      Assertions.assertTrue(System.currentTimeMillis() < deadline, "Timed out waiting for " + path);
      Thread.sleep(100);
    }
  }

  private boolean isAnnounced(final String path, final byte[] bytes) throws Exception
  {
    final Stat stat = new Stat();
    try {
      final byte[] data = curator.getData().decompressed().storingStatIn(stat).forPath(path);
      return Arrays.equals(bytes, data)
             && stat.getEphemeralOwner() == curator.getZookeeperClient().getZooKeeper().getSessionId();
    }
    catch (KeeperException e) {
      // Missing node, or the client is reconnecting.
      return false;
    }
  }

  /**
   * Blocks the ZooKeeper event thread of {@link #curator}, which delays all of its background callbacks and watch
   * notifications, until the returned latch is counted down.
   */
  public CountDownLatch blockEventThread() throws Exception
  {
    final CountDownLatch blocked = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    curator.checkExists().inBackground((client, event) -> {
      blocked.countDown();
      release.await(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
    }).forPath("/");
    Assertions.assertTrue(timing.awaitLatch(blocked));
    return release;
  }
}
