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

package org.apache.druid.consul.discovery;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.apache.http.NoHttpResponseException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsulDruidNodeAnnouncerTest
{
  private final DiscoveryDruidNode testNode = new DiscoveryDruidNode(
      new DruidNode("druid/broker", "test-host", true, 8082, null, true, false),
      NodeRole.BROKER,
      null
  );

  private final ConsulDiscoveryConfig config = TestUtils.builder()
      .servicePrefix("druid")
      .healthCheckInterval(Duration.millis(10000))
      .deregisterAfter(Duration.millis(90000))
      .watchSeconds(Duration.millis(60000))
      .watchRetryDelay(Duration.millis(10000))
      .build();

  private ConsulApiClient mockConsulApiClient;
  private ConsulDruidNodeAnnouncer announcer;

  @BeforeEach
  public void setUp()
  {
    mockConsulApiClient = EasyMock.createMock(ConsulApiClient.class);
  }

  @AfterEach
  public void tearDown()
  {
    // Don't call stop() here - each test will handle its own lifecycle
  }

  @Test
  public void testAnnounce() throws Exception
  {
    Capture<DiscoveryDruidNode> nodeCapture = Capture.newInstance();
    mockConsulApiClient.registerService(EasyMock.capture(nodeCapture));
    EasyMock.expectLastCall().once();

    // Expect health check TTL updates (may happen during test)
    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    // Expect deregisterService to be called during stop()
    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();
    announcer.announce(testNode);
    Assertions.assertEquals(testNode, nodeCapture.getValue());

    // Explicitly stop to trigger cleanup
    announcer.stop();

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testAnnounceRetriesTransientFailure() throws Exception
  {
    mockConsulApiClient.registerService(EasyMock.eq(testNode));
    EasyMock.expectLastCall().andThrow(new NoHttpResponseException("Consul did not respond"));

    mockConsulApiClient.registerService(EasyMock.eq(testNode));
    EasyMock.expectLastCall().once();

    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();

    announcer.announce(testNode);
    announcer.stop();

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testUnannounce() throws Exception
  {
    Capture<DiscoveryDruidNode> registerCapture = Capture.newInstance();
    mockConsulApiClient.registerService(EasyMock.capture(registerCapture));
    EasyMock.expectLastCall().once();

    // Expect deregisterService to be called once from unannounce()
    Capture<String> deregisterCapture = Capture.newInstance();
    mockConsulApiClient.deregisterService(EasyMock.capture(deregisterCapture));
    EasyMock.expectLastCall().once();

    // Expect health check TTL updates (may happen during test)
    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();
    announcer.announce(testNode);
    announcer.unannounce(testNode);

    EasyMock.verify(mockConsulApiClient);

    // Verify service ID format
    String expectedServiceId = "druid-broker-test-host-8082";
    Assertions.assertEquals(expectedServiceId, deregisterCapture.getValue());
  }

  @Test
  public void testAnnounceFails() throws Exception
  {
    mockConsulApiClient.registerService(EasyMock.anyObject(DiscoveryDruidNode.class));
    EasyMock.expectLastCall().andThrow(new RuntimeException("Consul unavailable"));

    // Expect health check TTL updates (may happen during test)
    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    // deregisterService expected as cleanup after failed announce
    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();

    try {
      announcer.announce(testNode);
      Assertions.fail("Expected RuntimeException");
    }
    catch (RuntimeException e) {
      Assertions.assertTrue(e.getMessage().contains("Failed to announce"));
    }

    // Don't need to stop since no nodes were announced

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testDuplicateAnnounceIsSkipped() throws Exception
  {
    // registerService should be called only once even if announce is invoked twice
    mockConsulApiClient.registerService(EasyMock.eq(testNode));
    EasyMock.expectLastCall().once();

    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    // stop() will deregister exactly once for the single announced node
    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();

    announcer.announce(testNode);
    announcer.announce(testNode); // should be a no-op

    announcer.stop();

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testAnnounceFailureTriggersCleanup() throws Exception
  {
    // First attempt fails, should trigger deregister cleanup; second attempt succeeds
    mockConsulApiClient.registerService(EasyMock.eq(testNode));
    EasyMock.expectLastCall().andThrow(new RuntimeException("boom"));

    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    mockConsulApiClient.registerService(EasyMock.eq(testNode));
    EasyMock.expectLastCall().once();

    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    // One successful announce means one deregister on stop
    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();

    try {
      announcer.announce(testNode);
      Assertions.fail("Expected failure on first announce");
    }
    catch (RuntimeException expected) {
      Assertions.assertTrue(expected.getMessage().contains("Failed to announce"));
    }

    announcer.announce(testNode); // succeeds

    announcer.stop();

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testConcurrentAnnounceOnlyRegistersOnce() throws Exception
  {
    // registerService should be called once despite concurrent announces
    mockConsulApiClient.registerService(EasyMock.eq(testNode));
    EasyMock.expectLastCall().once();

    mockConsulApiClient.passTtlCheck(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();

    mockConsulApiClient.deregisterService(EasyMock.anyString());
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsulApiClient);

    announcer = new ConsulDruidNodeAnnouncer(mockConsulApiClient, config);
    announcer.start();

    CountDownLatch latch = new CountDownLatch(2);

    Runnable announceTask = () -> {
      try {
        announcer.announce(testNode);
      }
      finally {
        latch.countDown();
      }
    };

    Thread t1 = new Thread(announceTask);
    Thread t2 = new Thread(announceTask);
    t1.start();
    t2.start();

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Announce tasks did not finish");

    announcer.stop();

    EasyMock.verify(mockConsulApiClient);
  }
}
