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

package org.apache.druid.curator.discovery;

import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorDruidLeaderSelectorTest extends CuratorTestBase
{
  private static final Logger logger = new Logger(CuratorDruidLeaderSelectorTest.class);

  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(EasyMock.createNiceMock(ServiceEmitter.class));
    setupServerAndCurator();
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  @Test(timeout = 60_000L)
  public void testSimple() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();

    final AtomicReference<DruidNode> currLeader = new AtomicReference<>();
    final String latchPath = "/testLatchPath";

    final DruidNode node1 = new DruidNode("service", "h1", false, 8080, null, true, false);
    final CuratorDruidLeaderSelector leaderSelector1 = new CuratorDruidLeaderSelector(curator, node1, latchPath);
    leaderSelector1.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            logger.info("listener1.becomeLeader().");
            currLeader.set(node1);
            throw new RuntimeException("I am Rogue.");
          }

          @Override
          public void stopBeingLeader()
          {
            logger.info("listener1.stopBeingLeader().");
            throw new RuntimeException("I said I am Rogue.");
          }
        }
    );

    // Wait until node1 has become leader
    while (!node1.equals(currLeader.get())) {
      logger.info("current leader = [%s]", currLeader.get());
      Thread.sleep(100);
    }

    Assert.assertFalse(leaderSelector1.isLeader());
    Assert.assertNull(leaderSelector1.getCurrentLeader());
    Assert.assertTrue(leaderSelector1.localTerm() >= 1);

    logger.info("Creating node2 leader selector");
    final DruidNode node2 = new DruidNode("service", "h2", false, 8080, null, true, false);
    final CuratorDruidLeaderSelector leaderSelector2 = new CuratorDruidLeaderSelector(curator, node2, latchPath);

    logger.info("Registering node2 listener");
    leaderSelector2.registerListener(
        new DruidLeaderSelector.Listener()
        {
          private final AtomicInteger attemptCount = new AtomicInteger(0);

          @Override
          public void becomeLeader()
          {
            logger.info("listener2.becomeLeader().");

            if (attemptCount.getAndIncrement() < 1) {
              throw new RuntimeException("will become leader on next attempt.");
            }

            currLeader.set(node2);
          }

          @Override
          public void stopBeingLeader()
          {
            logger.info("listener2.stopBeingLeader().");
            throw new RuntimeException("I am broken.");
          }
        }
    );

    // TODO: Why should node2 become leader at all?
    // Wait until node2 has become leader
    while (!node2.equals(currLeader.get())) {
      logger.info("current leader = [%s]", currLeader.get().getHostAndPortToUse());
      Thread.sleep(100);
    }

    Assert.assertTrue(leaderSelector2.isLeader());
    Assert.assertEquals("http://h2:8080", leaderSelector1.getCurrentLeader());
    Assert.assertEquals(2, leaderSelector2.localTerm());

    final DruidNode node3 = new DruidNode("service", "h3", false, 8080, null, true, false);
    final CuratorDruidLeaderSelector leaderSelector3 = new CuratorDruidLeaderSelector(curator, node3, latchPath);
    leaderSelector3.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            logger.info("listener3.becomeLeader().");
            currLeader.set(node3);
          }

          @Override
          public void stopBeingLeader()
          {
            logger.info("listener3.stopBeingLeader().");
          }
        }
    );

    // TODO: if we don't unregister 2, will 3 never become leader?
    //  Why should 1 not become leader again?
    leaderSelector2.unregisterListener();
    while (!node3.equals(currLeader.get())) {
      logger.info("current leader = [%s]", currLeader.get().getHostAndPortToUse());
      Thread.sleep(100);
    }

    Assert.assertTrue(leaderSelector3.isLeader());
    Assert.assertEquals("http://h3:8080", leaderSelector1.getCurrentLeader());
    Assert.assertEquals(1, leaderSelector3.localTerm());
  }

  @Test
  public void testDoesNotBecomeLeaderIfListenerFails() throws InterruptedException
  {
    curator.start();
    curator.blockUntilConnected();

    final String latchPath = "/testLatchPath";

    final DruidNode node1 = new DruidNode("service", "h1", false, 8080, null, true, false);
    final CuratorDruidLeaderSelector leaderSelector1 = new CuratorDruidLeaderSelector(curator, node1, latchPath);

    final CountDownLatch listenerBecomeLeaderInvoked = new CountDownLatch(1);
    leaderSelector1.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            listenerBecomeLeaderInvoked.countDown();
            throw new RuntimeException("Cannot become leader");
          }

          @Override
          public void stopBeingLeader()
          {
            throw new RuntimeException("Cannot stop being leader.");
          }
        }
    );

    // Wait until listener has been called
    listenerBecomeLeaderInvoked.await();

    logger.info("Kashif: going to do the verifications now");
    Assert.assertFalse(leaderSelector1.isLeader());
    Assert.assertNull(leaderSelector1.getCurrentLeader());
    Assert.assertTrue(leaderSelector1.localTerm() >= 1);
  }

  @Test
  public void testRecreatesLatchIfListenerFails()
  {

  }

}
