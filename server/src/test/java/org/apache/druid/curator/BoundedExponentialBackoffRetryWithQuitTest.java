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
import org.apache.curator.test.TestingServer;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public final class BoundedExponentialBackoffRetryWithQuitTest
{

  private static final Logger log = new Logger(BoundedExponentialBackoffRetryWithQuitTest.class);

  /*
  Methodology (order is important!):
    1. Zookeeper Server Service started
    2. Lifecycle started
    3. Curator invokes connection to service
    4. Service is stopped
    5. Curator attempts to do something, which invokes the retries policy
    6. Retries exceed limit, call function which simulates an exit (since mocking System.exit() is hard to do without
        changing a lot of dependencies)
   */
  @Test
  public void testExitWithLifecycle() throws Exception
  {
    final Lifecycle actualNoop = new Lifecycle() {
      @Override
      public void start() throws Exception
      {
        super.start();
        log.info("Starting lifecycle...");
      }

      @Override
      public void stop()
      {
        super.stop();
        log.info("Stopping lifecycle...");
      }
    };
    Lifecycle noop = EasyMock.mock(Lifecycle.class);

    noop.start();
    EasyMock.expectLastCall().andDelegateTo(actualNoop);
    noop.stop();
    EasyMock.expectLastCall().andDelegateTo(actualNoop);
    EasyMock.replay(noop);

    Runnable exitFunction = () -> {
      log.info("Zookeeper retries exhausted, exiting...");
      noop.stop();
      throw new RuntimeException("Simulated exit");
    };

    TestingServer server = new TestingServer();
    BoundedExponentialBackoffRetryWithQuit retry = new BoundedExponentialBackoffRetryWithQuit(exitFunction, 1, 1, 2);
    CuratorFramework curator = CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(1000)
        .connectionTimeoutMs(1)
        .retryPolicy(retry)
        .build();
    server.start();
    System.out.println("Server started.");
    curator.start();
    noop.start();
    curator.checkExists().forPath("/tmp");
    log.info("Connected.");
    boolean failed = false;
    try {
      server.stop();
      log.info("Stopped.");
      curator.checkExists().forPath("/tmp");
      Thread.sleep(10);
      curator.checkExists().forPath("/tmp");
    }
    catch (Exception e) {
      Assert.assertTrue("Correct exception type", e instanceof RuntimeException);
      EasyMock.verify(noop);
      curator.close();
      failed = true;
    }
    Assert.assertTrue("Must be marked in failure state", failed);
    log.info("Lifecycle stopped.");
  }

}
