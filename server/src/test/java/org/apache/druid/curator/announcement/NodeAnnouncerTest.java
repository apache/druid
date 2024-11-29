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

import com.google.common.collect.Sets;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.test.KillSession;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.ZKPathsUtils;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class NodeAnnouncerTest extends CuratorTestBase
{
  private static final Logger log = new Logger(NodeAnnouncerTest.class);
  private ExecutorService exec;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    exec = Execs.singleThreaded("test-node-announcer-sanity-%s");
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  @Test
  public void testAnnounceBeforeStartingNodeAnnouncer() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/testAnnounce";

    announcer.announce(testPath, billy);
    announcer.start();

    // Verify that the path was announced
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test(timeout = 60_000L)
  public void testCreateParentPath() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/newParent/testPath";
    final String parentPath = ZKPathsUtils.getParentPath(testPath);

    announcer.start();
    Assert.assertNull("Parent path should not exist before announcement", curator.checkExists().forPath(parentPath));
    announcer.announce(testPath, billy);

    // Wait for the announcement to be processed
    while (curator.checkExists().forPath(testPath) == null) {
      Thread.sleep(100);
    }

    Assert.assertNotNull("Parent path should be created", curator.checkExists().forPath(parentPath));
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test(timeout = 60_000L)
  public void testAnnounceSamePathWithDifferentPayloadThrowsIAE() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final byte[] tilly = StringUtils.toUtf8("tilly");
    final String testPath = "/testPath";

    announcer.start();
    announcer.announce(testPath, billy);
    while (curator.checkExists().forPath(testPath) == null) {
      Thread.sleep(100);
    }
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));

    // Nothing wrong when we announce same path.
    announcer.announce(testPath, billy);

    // Something wrong when we announce different path.
    Exception exception = Assert.assertThrows(IAE.class, () -> announcer.announce(testPath, tilly));
    Assert.assertEquals(exception.getMessage(), "Cannot reannounce different values under the same path.");

    // Confirm that the new announcement is invalidated, and we still have payload from previous announcement.
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  public void testUpdateBeforeStartingNodeAnnouncer() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final byte[] tilly = StringUtils.toUtf8("tilly");
    final String testPath = "/testAnnounce";

    announcer.update(testPath, tilly);
    announcer.announce(testPath, billy);
    announcer.start();

    // Verify that the path was announced
    Assert.assertArrayEquals(tilly, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  public void testUpdateSuccessfully() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final byte[] tilly = StringUtils.toUtf8("tilly");
    final String testPath = "/testUpdate";

    announcer.start();
    announcer.announce(testPath, billy);
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));

    announcer.update(testPath, billy);
    Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));

    announcer.update(testPath, tilly);
    Assert.assertArrayEquals(tilly, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  public void testUpdateWithNonExistentPath() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/testUpdate";

    announcer.start();

    Exception exception = Assert.assertThrows(ISE.class, () -> announcer.update(testPath, billy));
    Assert.assertEquals(exception.getMessage(), "Cannot update path[/testUpdate] that hasn't been announced!");
    announcer.stop();
  }

  @Test(timeout = 60_000L)
  public void testSanity() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath1 = "/test1";
    final String testPath2 = "/somewhere/test2";
    announcer.announce(testPath1, billy);

    Assert.assertNull("/test1 does not exists", curator.checkExists().forPath(testPath1));
    Assert.assertNull("/somewhere/test2 does not exists", curator.checkExists().forPath(testPath2));

    announcer.start();
    while (!announcer.getAddedPaths().contains("/test1")) {
      Thread.sleep(100);
    }

    try {
      Assert.assertArrayEquals("/test1 has data", billy, curator.getData().decompressed().forPath(testPath1));
      Assert.assertNull("/somewhere/test2 still does not exist", curator.checkExists().forPath(testPath2));

      announcer.announce(testPath2, billy);

      Assert.assertArrayEquals("/test1 still has data", billy, curator.getData().decompressed().forPath(testPath1));
      Assert.assertArrayEquals(
          "/somewhere/test2 has data",
          billy,
          curator.getData().decompressed().forPath(testPath2)
      );

      final CountDownLatch latch = new CountDownLatch(1);
      curator.getCuratorListenable().addListener(
          (client, event) -> {
            if (event.getType() == CuratorEventType.CREATE && event.getPath().equals(testPath1)) {
              latch.countDown();
            }
          }
      );
      final CuratorOp deleteOp = curator.transactionOp().delete().forPath(testPath1);
      final Collection<CuratorTransactionResult> results = curator.transaction().forOperations(deleteOp);
      Assert.assertEquals(1, results.size());
      final CuratorTransactionResult result = results.iterator().next();
      Assert.assertEquals(Code.OK.intValue(), result.getError()); // assert delete

      Assert.assertTrue("Wait for /test1 to be created", timing.forWaiting().awaitLatch(latch));

      Assert.assertArrayEquals(
          "expect /test1 data is restored",
          billy,
          curator.getData().decompressed().forPath(testPath1)
      );
      Assert.assertArrayEquals(
          "expect /somewhere/test2 is still there",
          billy,
          curator.getData().decompressed().forPath(testPath2)
      );

      announcer.unannounce(testPath1);
      Assert.assertNull("expect /test1 unannounced", curator.checkExists().forPath(testPath1));
      Assert.assertArrayEquals(
          "expect /somewhere/test2 is still still there",
          billy,
          curator.getData().decompressed().forPath(testPath2)
      );
    }
    finally {
      announcer.stop();
    }

    Assert.assertNull("expect /test1 remains unannounced", curator.checkExists().forPath(testPath1));
    Assert.assertNull("expect /somewhere/test2 unannounced", curator.checkExists().forPath(testPath2));
  }

  @Test(timeout = 60_000L)
  public void testSessionKilled() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    try {
      curator.inTransaction().create().forPath("/somewhere").and().commit();
      announcer.start();

      final byte[] billy = StringUtils.toUtf8("billy");
      final String testPath1 = "/test1";
      final String testPath2 = "/somewhere/test2";
      final Set<String> paths = Sets.newHashSet(testPath1, testPath2);
      announcer.announce(testPath1, billy);
      announcer.announce(testPath2, billy);

      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

      final CountDownLatch latch = new CountDownLatch(1);
      curator.getCuratorListenable().addListener(
          (client, event) -> {
            if (event.getType() == CuratorEventType.CREATE) {
              paths.remove(event.getPath());
              if (paths.isEmpty()) {
                latch.countDown();
              }
            }
          }
      );
      KillSession.kill(curator.getZookeeperClient().getZooKeeper(), server.getConnectString());

      Assert.assertTrue(timing.forWaiting().awaitLatch(latch));

      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assert.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

      announcer.stop();

      while ((curator.checkExists().forPath(testPath1) != null) || (curator.checkExists().forPath(testPath2) != null)) {
        Thread.sleep(100);
      }

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
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test2";
    final String parent = ZKPathsUtils.getParentPath(testPath);

    announcer.start();
    try {
      Assert.assertNull(curator.checkExists().forPath(parent));

      awaitAnnounce(announcer, testPath, billy, true);

      Assert.assertNotNull(curator.checkExists().forPath(parent));
    }
    finally {
      announcer.stop();
    }

    Assert.assertNull(curator.checkExists().forPath(parent));
  }

  @Test
  public void testLeavesBehindTurdlingsThatAlreadyExisted() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test2";
    final String parent = ZKPathsUtils.getParentPath(testPath);

    curator.create().forPath(parent);
    final Stat initialStat = curator.checkExists().forPath(parent);

    announcer.start();
    try {
      Assert.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());

      awaitAnnounce(announcer, testPath, billy, true);

      Assert.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());
    }
    finally {
      announcer.stop();
    }

    Assert.assertEquals(initialStat.getMzxid(), curator.checkExists().forPath(parent).getMzxid());
  }

  @Test
  public void testLeavesBehindTurdlingsWhenToldTo() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test2";
    final String parent = ZKPathsUtils.getParentPath(testPath);

    announcer.start();
    try {
      Assert.assertNull(curator.checkExists().forPath(parent));

      awaitAnnounce(announcer, testPath, billy, false);

      Assert.assertNotNull(curator.checkExists().forPath(parent));
    }
    finally {
      announcer.stop();
    }

    Assert.assertNotNull(curator.checkExists().forPath(parent));
  }

  private void awaitAnnounce(
      final NodeAnnouncer announcer,
      final String path,
      final byte[] bytes,
      boolean removeParentsIfCreated
  ) throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    curator.getCuratorListenable().addListener(
        (client, event) -> {
          if (event.getType() == CuratorEventType.CREATE && event.getPath().equals(path)) {
            latch.countDown();
          }
        }
    );
    announcer.announce(path, bytes, removeParentsIfCreated);
    latch.await();
  }
}
