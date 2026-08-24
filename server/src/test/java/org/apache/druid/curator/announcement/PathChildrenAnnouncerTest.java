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

package org.apache.druid.curator.announcement;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.test.KillSession;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class PathChildrenAnnouncerTest extends CuratorTestBase
{
  private static final Logger log = new Logger(PathChildrenAnnouncerTest.class);
  private ExecutorService exec;

  @BeforeEach
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    exec = Execs.singleThreaded("test-announcer-sanity-%s");
    curator.start();
    curator.blockUntilConnected();
  }

  @AfterEach
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSanity() throws Exception
  {
    PathChildrenAnnouncer announcer = new PathChildrenAnnouncer(curator, exec);
    announcer.initializeAddedChildren();

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath1 = "/test1";
    final String testPath2 = "/somewhere/test2";
    announcer.announce(testPath1, billy);

    Assertions.assertNull(curator.checkExists().forPath(testPath1), "/test1 does not exists");
    Assertions.assertNull(curator.checkExists().forPath(testPath2), "/somewhere/test2 does not exists");

    announcer.start();
    while (!announcer.getAddedChildren().contains("/test1")) {
      Thread.sleep(100);
    }

    try {
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1), "/test1 has data");
      Assertions.assertNull(curator.checkExists().forPath(testPath2), "/somewhere/test2 still does not exist");

      announcer.announce(testPath2, billy);

      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1), "/test1 still has data");
      Assertions.assertArrayEquals(
          billy,
          curator.getData().decompressed().forPath(testPath2),
          "/somewhere/test2 has data"
      );

      final CountDownLatch latch = createCountdownLatchForPaths(testPath1);

      final CuratorOp deleteOp = curator.transactionOp().delete().forPath(testPath1);
      final Collection<CuratorTransactionResult> results = curator.transaction().forOperations(deleteOp);
      Assertions.assertEquals(1, results.size());
      final CuratorTransactionResult result = results.iterator().next();
      Assertions.assertEquals(Code.OK.intValue(), result.getError()); // assert delete

      Assertions.assertTrue(timing.forWaiting().awaitLatch(latch), "Wait for /test1 to be created");

      Assertions.assertArrayEquals(
          billy,
          curator.getData().decompressed().forPath(testPath1),
          "expect /test1 data is restored"
      );
      Assertions.assertArrayEquals(
          billy,
          curator.getData().decompressed().forPath(testPath2),
          "expect /somewhere/test2 is still there"
      );

      announcer.unannounce(testPath1);
      Assertions.assertNull(curator.checkExists().forPath(testPath1), "expect /test1 unannounced");
      Assertions.assertArrayEquals(
          billy,
          curator.getData().decompressed().forPath(testPath2),
          "expect /somewhere/test2 is still still there"
      );
    }
    finally {
      announcer.stop();
    }

    Assertions.assertNull(curator.checkExists().forPath(testPath1), "expect /test1 remains unannounced");
    Assertions.assertNull(curator.checkExists().forPath(testPath2), "expect /somewhere/test2 unannounced");
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSessionKilled() throws Exception
  {
    PathChildrenAnnouncer announcer = new PathChildrenAnnouncer(curator, exec);
    try {
      CuratorOp createOp = curator.transactionOp().create().forPath("/somewhere");
      curator.transaction().forOperations(createOp);
      announcer.start();

      final byte[] billy = StringUtils.toUtf8("billy");
      final String testPath1 = "/test1";
      final String testPath2 = "/somewhere/test2";
      final String[] paths = new String[]{testPath1, testPath2};
      announcer.announce(testPath1, billy);
      announcer.announce(testPath2, billy);

      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

      final CountDownLatch latch = createCountdownLatchForPaths(paths);

      KillSession.kill(curator.getZookeeperClient().getZooKeeper(), server.getConnectString());

      Assertions.assertTrue(timing.forWaiting().awaitLatch(latch));

      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

      announcer.stop();

      while ((curator.checkExists().forPath(testPath1) != null) || (curator.checkExists().forPath(testPath2) != null)) {
        Thread.sleep(100);
      }

      Assertions.assertNull(curator.checkExists().forPath(testPath1));
      Assertions.assertNull(curator.checkExists().forPath(testPath2));
    }
    finally {
      announcer.stop();
    }
  }

  @Test
  public void testRemovesParentIfCreated() throws Exception
  {
    PathChildrenAnnouncer announcer = new PathChildrenAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test2";
    final String parent = ZKPaths.getPathAndNode(testPath).getPath();

    announcer.start();
    try {
      Assertions.assertNull(curator.checkExists().forPath(parent));

      awaitAnnounce(announcer, testPath, billy, true);

      Assertions.assertNotNull(curator.checkExists().forPath(parent));
    }
    finally {
      announcer.stop();
    }

    Assertions.assertNull(curator.checkExists().forPath(parent));
  }

  @Test
  public void testLeavesBehindParentPathIfAlreadyExists() throws Exception
  {
    PathChildrenAnnouncer announcer = new PathChildrenAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test";
    final String parent = ZKPaths.getPathAndNode(testPath).getPath();

    curator.create().forPath(parent);
    final Stat initialStat = curator.checkExists().forPath(parent);

    announcer.start();
    try {
      Assertions.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());

      awaitAnnounce(announcer, testPath, billy, true);

      Assertions.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());
    }
    finally {
      announcer.stop();
    }

    Assertions.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());
  }

  @Test
  public void testLeavesParentPathsUntouchedWhenInstructed() throws Exception
  {
    PathChildrenAnnouncer announcer = new PathChildrenAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test";
    final String parent = ZKPaths.getPathAndNode(testPath).getPath();

    announcer.start();
    try {
      Assertions.assertNull(curator.checkExists().forPath(parent));

      awaitAnnounce(announcer, testPath, billy, false);

      Assertions.assertNotNull(curator.checkExists().forPath(parent));
    }
    finally {
      announcer.stop();
    }

    Assertions.assertNotNull(curator.checkExists().forPath(parent));
  }

  private void awaitAnnounce(
      final PathChildrenAnnouncer announcer,
      final String path,
      final byte[] bytes,
      boolean removeParentsIfCreated
  ) throws InterruptedException
  {
    CountDownLatch latch = createCountdownLatchForPaths(path);
    announcer.announce(path, bytes, removeParentsIfCreated);
    latch.await();
  }

  private CountDownLatch createCountdownLatchForPaths(String... path)
  {
    final CountDownLatch latch = new CountDownLatch(path.length);
    curator.getCuratorListenable().addListener(
        new CuratorListener()
        {
          @Override
          public void eventReceived(CuratorFramework client, CuratorEvent event)
          {
            if (event.getType() == CuratorEventType.CREATE && Arrays.asList(path).contains(event.getPath())) {
              latch.countDown();
            }
          }
        }
    );

    return latch;
  }
}
