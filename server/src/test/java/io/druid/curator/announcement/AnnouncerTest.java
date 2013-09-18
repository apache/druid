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

package io.druid.curator.announcement;

import com.google.common.collect.Sets;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.test.KillSession;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 */
public class AnnouncerTest extends CuratorTestBase
{

  private ExecutorService exec;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    exec = Execs.singleThreaded("test-announcer-sanity-%s");
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  @Test
  public void testSanity() throws Exception
  {
    curator.start();
    Announcer announcer = new Announcer(curator, exec);

    final byte[] billy = "billy".getBytes();
    final String testPath1 = "/test1";
    final String testPath2 = "/somewhere/test2";
    announcer.announce(testPath1, billy);

    Assert.assertNull(curator.checkExists().forPath(testPath1));
    Assert.assertNull(curator.checkExists().forPath(testPath2));

    announcer.start();

    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
    Assert.assertNull(curator.checkExists().forPath(testPath2));

    announcer.announce(testPath2, billy);

    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

    final CountDownLatch latch = new CountDownLatch(1);
    curator.getCuratorListenable().addListener(
        new CuratorListener()
        {
          @Override
          public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
          {
            if (event.getType() == CuratorEventType.CREATE && event.getPath().equals(testPath1)) {
              latch.countDown();
            }
          }
        }
    );
    curator.delete().forPath(testPath1);
    Assert.assertTrue(timing.awaitLatch(latch));

    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

    announcer.unannounce(testPath1);
    Assert.assertNull(curator.checkExists().forPath(testPath1));
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

    announcer.stop();

    Assert.assertNull(curator.checkExists().forPath(testPath1));
    Assert.assertNull(curator.checkExists().forPath(testPath2));
  }

  @Test
  public void testSessionKilled() throws Exception
  {
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

      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

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

      Assert.assertTrue(timing.awaitLatch(latch));

      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

      announcer.stop();

      Assert.assertNull(curator.checkExists().forPath(testPath1));
      Assert.assertNull(curator.checkExists().forPath(testPath2));
    }
    finally {
      announcer.stop();
    }
  }

  @Test
  public void testCleansUpItsLittleTurdlings() throws Exception
  {
    curator.start();
    Announcer announcer = new Announcer(curator, exec);

    final byte[] billy = "billy".getBytes();
    final String testPath = "/somewhere/test2";
    final String parent = ZKPaths.getPathAndNode(testPath).getPath();

    announcer.start();

    Assert.assertNull(curator.checkExists().forPath(parent));

    announcer.announce(testPath, billy);

    Assert.assertNotNull(curator.checkExists().forPath(parent));

    announcer.stop();

    Assert.assertNull(curator.checkExists().forPath(parent));
  }

  @Test
  public void testLeavesBehindTurdlingsThatAlreadyExisted() throws Exception
  {
    curator.start();
    Announcer announcer = new Announcer(curator, exec);

    final byte[] billy = "billy".getBytes();
    final String testPath = "/somewhere/test2";
    final String parent = ZKPaths.getPathAndNode(testPath).getPath();

    curator.create().forPath(parent);
    final Stat initialStat = curator.checkExists().forPath(parent);

    announcer.start();

    Assert.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());

    announcer.announce(testPath, billy);

    Assert.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());

    announcer.stop();

    Assert.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());
  }

}
