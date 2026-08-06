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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class CuratorDruidLeaderSelectorTest extends CuratorTestBase
{
  private static final Logger logger = new Logger(CuratorDruidLeaderSelectorTest.class);

  private final StubServiceEmitter emitter = new StubServiceEmitter();

  @BeforeEach
  public void setUp() throws Exception
  {
    emitter.flush();
    EmittingLogger.registerEmitter(emitter);
    setupServerAndCurator();
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSimple() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();

    AtomicReference<String> currLeader = new AtomicReference<>();

    String latchPath = "/testlatchPath";

    CuratorDruidLeaderSelector leaderSelector1 = new CuratorDruidLeaderSelector(
        curator,
        new DruidNode("s1", "h1", false, 8080, null, true, false),
        latchPath
    );
    leaderSelector1.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            logger.info("listener1.becomeLeader().");
            currLeader.set("h1:8080");
            throw new RuntimeException("I am Rogue.");
          }

          @Override
          public void stopBeingLeader()
          {
            logger.info("listener1.stopBeingLeader().");
          }
        }
    );

    while (!"h1:8080".equals(currLeader.get())) {
      logger.info("current leader = [%s]", currLeader.get());
      Thread.sleep(100);
    }

    Assertions.assertTrue(leaderSelector1.localTerm() >= 1);

    CuratorDruidLeaderSelector leaderSelector2 = new CuratorDruidLeaderSelector(
        curator,
        new DruidNode("s2", "h2", false, 8080, null, true, false),
        latchPath
    );
    leaderSelector2.registerListener(
        new DruidLeaderSelector.Listener()
        {
          private AtomicInteger attemptCount = new AtomicInteger(0);

          @Override
          public void becomeLeader()
          {
            logger.info("listener2.becomeLeader().");

            if (attemptCount.getAndIncrement() < 1) {
              throw new RuntimeException("will become leader on next attempt.");
            }

            currLeader.set("h2:8080");
          }

          @Override
          public void stopBeingLeader()
          {
            logger.info("listener2.stopBeingLeader().");
          }
        }
    );

    while (!"h2:8080".equals(currLeader.get())) {
      logger.info("current leader = [%s]", currLeader.get());
      Thread.sleep(100);
    }

    Assertions.assertTrue(leaderSelector2.isLeader());
    Assertions.assertEquals("http://h2:8080", leaderSelector1.getCurrentLeader());
    Assertions.assertEquals(2, leaderSelector2.localTerm());

    CuratorDruidLeaderSelector leaderSelector3 = new CuratorDruidLeaderSelector(
        curator,
        new DruidNode("s3", "h3", false, 8080, null, true, false),
        latchPath
    );
    leaderSelector3.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            logger.info("listener3.becomeLeader().");
            currLeader.set("h3:8080");
          }

          @Override
          public void stopBeingLeader()
          {
            logger.info("listener3.stopBeingLeader().");
          }
        }
    );

    leaderSelector2.unregisterListener();
    while (!"h3:8080".equals(currLeader.get())) {
      logger.info("current leader = [%s]", currLeader.get());
      Thread.sleep(100);
    }

    Assertions.assertTrue(leaderSelector3.isLeader());
    Assertions.assertEquals("http://h3:8080", leaderSelector1.getCurrentLeader());
    Assertions.assertEquals(1, leaderSelector3.localTerm());
  }

  @Test
  @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
  public void test_becomeLeader_triggersCleanup_onFailure() throws InterruptedException
  {
    curator.start();
    curator.blockUntilConnected();

    final CuratorDruidLeaderSelector leaderSelector = new CuratorDruidLeaderSelector(
        curator,
        new DruidNode("s1", "h1", false, 8080, null, true, false),
        "/testLatchPath"
    );

    final AtomicInteger becomeLeaderCalled = new AtomicInteger();
    final AtomicInteger stopBeingLeaderCalled = new AtomicInteger();
    leaderSelector.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            becomeLeaderCalled.incrementAndGet();
            throw new ISE("Fail to become leader");
          }

          @Override
          public void stopBeingLeader()
          {
            stopBeingLeaderCalled.incrementAndGet();
          }
        }
    );

    while (becomeLeaderCalled.get() == 0) {
      logger.info("Waiting to become leader");
      Thread.sleep(100);
    }

    Assertions.assertEquals(1, becomeLeaderCalled.get());
    Assertions.assertEquals(1, stopBeingLeaderCalled.get());
  }

  @AfterEach
  public void tearDown()
  {
    tearDownServerAndCurator();
  }
}
