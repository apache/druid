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

import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.test.KillSession;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
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

public class NodeAnnouncerTest extends CuratorTestBase
{
  private ExecutorService exec;

  @BeforeEach
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    exec = Execs.singleThreaded("test-node-announcer-sanity-%s");
    curator.start();
    curator.blockUntilConnected();
  }

  @AfterEach
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  @Test
  @Timeout(60_000)
  public void testCreateParentPath() throws Exception
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/newParent/testPath";
    final String parentPath = ZKPaths.getPathAndNode(testPath).getPath();

    announcer.start();
    Assertions.assertNull(curator.checkExists().forPath(parentPath), "Parent path should not exist before announcement");
    announcer.announce(testPath, billy);

    // Wait for the announcement to be processed
    while (curator.checkExists().forPath(testPath) == null) {
      Thread.sleep(100);
    }

    Assertions.assertNotNull(curator.checkExists().forPath(parentPath), "Parent path should be created");
    Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  @Timeout(60_000)
  public void testAnnounceSamePathWithDifferentPayloadThrowsIAE() throws Exception
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final byte[] tilly = StringUtils.toUtf8("tilly");
    final String testPath = "/testPath";

    announcer.start();
    announcer.announce(testPath, billy);
    while (curator.checkExists().forPath(testPath) == null) {
      Thread.sleep(100);
    }
    Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));

    // Nothing wrong when we announce same payload on the same path.
    announcer.announce(testPath, billy);

    // Expect an exception when announcing a different payload
    IAE exception = Assertions.assertThrows(IAE.class, () -> announcer.announce(testPath, tilly));
    Assertions.assertEquals("Cannot reannounce different values under the same path.", exception.getMessage());

    // Confirm that the announcement remains unchanged.
    Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  public void testUpdateBeforeStartingNodeAnnouncer() throws Exception
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final byte[] tilly = StringUtils.toUtf8("tilly");
    final String testPath = "/testAnnounce";

    // Queue update before the announcer is started
    announcer.update(testPath, tilly);
    announcer.announce(testPath, billy);
    announcer.start();

    // Verify that the update took precedence
    Assertions.assertArrayEquals(tilly, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  public void testUpdateSuccessfully() throws Exception
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final byte[] tilly = StringUtils.toUtf8("tilly");
    final String testPath = "/testUpdate";

    announcer.start();
    announcer.announce(testPath, billy);
    Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));

    // Update with the same payload: nothing should change.
    announcer.update(testPath, billy);
    Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath));

    // Update with a new payload.
    announcer.update(testPath, tilly);
    Assertions.assertArrayEquals(tilly, curator.getData().decompressed().forPath(testPath));
    announcer.stop();
  }

  @Test
  public void testUpdateNonExistentPath()
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/testUpdate";

    announcer.start();

    ISE exception = Assertions.assertThrows(ISE.class, () -> announcer.update(testPath, billy));
    Assertions.assertEquals("Cannot update path[/testUpdate] that hasn't been announced!", exception.getMessage());
    announcer.stop();
  }

  @Test
  @Timeout(60_000)
  public void testSanity() throws Exception
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath1 = "/test1";
    final String testPath2 = "/somewhere/test2";
    announcer.announce(testPath1, billy);

    Assertions.assertNull(curator.checkExists().forPath(testPath1), "/test1 does not exist before announcer start");
    Assertions.assertNull(curator.checkExists().forPath(testPath2), "/somewhere/test2 does not exist before announcer start");

    announcer.start();
    while (!announcer.getAddedPaths().contains("/test1")) {
      Thread.sleep(100);
    }

    try {
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1), "/test1 has data");
      Assertions.assertNull(curator.checkExists().forPath(testPath2), "/somewhere/test2 still does not exist");

      announcer.announce(testPath2, billy);

      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1), "/test1 still has data");
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2), "/somewhere/test2 has data");

      final CountDownLatch latch = new CountDownLatch(1);
      curator.getCuratorListenable().addListener((client, event) -> {
        if (event.getType() == CuratorEventType.CREATE && event.getPath().equals(testPath1)) {
          latch.countDown();
        }
      });
      final CuratorOp deleteOp = curator.transactionOp().delete().forPath(testPath1);
      final Collection<CuratorTransactionResult> results = curator.transaction().forOperations(deleteOp);
      Assertions.assertEquals(1, results.size(), "Expected one result from the delete op");
      final CuratorTransactionResult result = results.iterator().next();
      Assertions.assertEquals(Code.OK.intValue(), result.getError(), "Expected OK code on delete");

      Assertions.assertTrue(timing.forWaiting().awaitLatch(latch), "Wait for /test1 to be recreated");

      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1), "Expected /test1 data to be restored");
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2), "Expected /somewhere/test2 data to remain");

      announcer.unannounce(testPath1);
      Assertions.assertNull(curator.checkExists().forPath(testPath1), "Expected /test1 to be unannounced");
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2), "Expected /somewhere/test2 to remain");
    }
    finally {
      announcer.stop();
    }

    Assertions.assertNull(curator.checkExists().forPath(testPath1), "Expected /test1 to remain unannounced");
    Assertions.assertNull(curator.checkExists().forPath(testPath2), "Expected /somewhere/test2 to be unannounced");
  }

  @Test
  @Timeout(60_000)
  public void testSessionKilled() throws Exception
  {
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);
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

      Assertions.assertTrue(timing.forWaiting().awaitLatch(latch), "Await latch after killing session");

      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath1));
      Assertions.assertArrayEquals(billy, curator.getData().decompressed().forPath(testPath2));

      announcer.stop();

      while ((curator.checkExists().forPath(testPath1) != null) ||
              (curator.checkExists().forPath(testPath2) != null)) {
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
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test";
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
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test2";
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
    NodeAnnouncer announcer = new NodeAnnouncer(curator, exec);

    final byte[] billy = StringUtils.toUtf8("billy");
    final String testPath = "/somewhere/test2";
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
          final NodeAnnouncer announcer,
          final String path,
          final byte[] bytes,
          boolean removeParentsIfCreated
  ) throws InterruptedException
  {
    final CountDownLatch latch = createCountdownLatchForPaths(path);
    announcer.announce(path, bytes, removeParentsIfCreated);
    latch.await();
  }

  private CountDownLatch createCountdownLatchForPaths(String... paths)
  {
    final CountDownLatch latch = new CountDownLatch(paths.length);
    curator.getCuratorListenable().addListener((client, event) -> {
      if (event.getType() == CuratorEventType.CREATE && Arrays.asList(paths).contains(event.getPath())) {
        latch.countDown();
      }
    });

    return latch;
  }
}
