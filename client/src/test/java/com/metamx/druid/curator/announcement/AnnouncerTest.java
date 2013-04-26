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

package com.metamx.druid.curator.announcement;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.metamx.druid.concurrent.Execs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 */
public class AnnouncerTest
{

  private TestingServer server;

  @Before
  public void setUp() throws Exception
  {
    server = new TestingServer();
  }

  @Test
  public void testSanity() throws Exception
  {
    Timing timing = new Timing();
    CuratorFramework curator = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
    final ExecutorService exec = Execs.singleThreaded("test-announcer-sanity-%s");

    curator.start();
    try {
      curator.create().forPath("/somewhere");
      Announcer announcer = new Announcer(curator, exec);

      final byte[] billy = "billy".getBytes();
      final String testPath1 = "/test1";
      final String testPath2 = "/somewhere/test2";
      announcer.announce(testPath1, billy);

      Assert.assertNull(curator.checkExists().forPath(testPath1));
      Assert.assertNull(curator.checkExists().forPath(testPath2));

      announcer.start();

      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath1));
      Assert.assertNull(curator.checkExists().forPath(testPath2));

      announcer.announce(testPath2, billy);

      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath2));

      curator.delete().forPath(testPath1);
      Thread.sleep(20); // Give the announcer time to notice

      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath2));

      announcer.unannounce(testPath1);
      Assert.assertNull(curator.checkExists().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath2));

      announcer.stop();

      Assert.assertNull(curator.checkExists().forPath(testPath1));
      Assert.assertNull(curator.checkExists().forPath(testPath2));
    }
    finally {
      Closeables.closeQuietly(curator);
    }
  }

  @Test
  public void testSessionKilled() throws Exception
  {
    Timing timing = new Timing();
    CuratorFramework curator = CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(timing.session())
        .connectionTimeoutMs(timing.connection())
        .retryPolicy(new RetryOneTime(1))
        .build();

    final ExecutorService exec = Execs.singleThreaded("test-announcer-sanity-%s");

    curator.start();
    Announcer announcer = new Announcer(curator, exec);
    try {
      curator.create().forPath("/somewhere");
      announcer.start();

      final byte[] billy = "billy".getBytes();
      final String testPath1 = "/test1";
      final String testPath2 = "/somewhere/test2";
      final Set<String> paths = Sets.newHashSet(testPath1, testPath2);
      announcer.announce(testPath1, billy);
      announcer.announce(testPath2, billy);

      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath2));

      final CountDownLatch latch = new CountDownLatch(1);
      curator.getCuratorListenable().addListener(
          new CuratorListener()
          {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
            {
              if (event.getType() == CuratorEventType.CREATE) {
                paths.remove(event.getPath());
                if (paths.isEmpty()) {
                  latch.countDown();
                }
              }
            }
          }
      );
      KillSession.kill(curator.getZookeeperClient().getZooKeeper(), server.getConnectString());

      timing.awaitLatch(latch);

      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().forPath(testPath2));

      announcer.stop();

      Assert.assertNull(curator.checkExists().forPath(testPath1));
      Assert.assertNull(curator.checkExists().forPath(testPath2));
    }
    finally {
      announcer.stop();
      Closeables.closeQuietly(curator);
    }
  }
}
