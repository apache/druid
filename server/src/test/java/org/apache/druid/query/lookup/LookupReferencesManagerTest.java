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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.coordinator.CoordinatorClientImpl;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LookupReferencesManagerTest
{
  private static final String LOOKUP_TIER = "lookupTier";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  LookupReferencesManager lookupReferencesManager;
  LookupExtractorFactory lookupExtractorFactory;
  LookupExtractorFactoryContainer container;
  ObjectMapper mapper = new DefaultObjectMapper();
  private CoordinatorClientImpl coordinatorClient;
  private LookupListeningAnnouncerConfig config;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    coordinatorClient = EasyMock.createMock(CoordinatorClientImpl.class);

    config = EasyMock.createMock(LookupListeningAnnouncerConfig.class);
    EasyMock.expect(config.getLookupLoadingSpec()).andReturn(LookupLoadingSpec.ALL).anyTimes();

    lookupExtractorFactory = new MapLookupExtractorFactory(
        ImmutableMap.of(
            "key",
            "value"
        ), true
    );
    container = new LookupExtractorFactoryContainer("v0", lookupExtractorFactory);
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(temporaryFolder.newFolder().getAbsolutePath()),
        mapper,
        coordinatorClient,
        config,
        true
    );
  }

  @Test
  public void testStartStop() throws IOException
  {
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(null),
        mapper, coordinatorClient, config
    );

    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForStartStop", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    Assert.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertNull(lookupReferencesManager.mainThread);
    Assert.assertNull(lookupReferencesManager.stateRef.get());

    lookupReferencesManager.start();
    Assert.assertTrue(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertTrue(lookupReferencesManager.mainThread.isAlive());
    Assert.assertNotNull(lookupReferencesManager.stateRef.get());

    lookupReferencesManager.stop();
    Assert.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertFalse(lookupReferencesManager.mainThread.isAlive());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetExceptionWhenClosed()
  {
    lookupReferencesManager.get("test");
  }

  @Test(expected = IllegalStateException.class)
  public void testAddExceptionWhenClosed()
  {
    lookupReferencesManager.add("test", EasyMock.createMock(LookupExtractorFactoryContainer.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testRemoveExceptionWhenClosed()
  {
    lookupReferencesManager.remove("test", null);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetAllLookupsStateExceptionWhenClosed()
  {
    lookupReferencesManager.getAllLookupsState();
  }

  @Test
  public void testAddGetRemove() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.replay(lookupExtractorFactory);

    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));
  }

  @Test
  public void testLoadBadContaineAfterOldGoodContainer() throws Exception
  {
    // Test the scenario of not loading the new container until it get intialized
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.replay(lookupExtractorFactory);

    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    LookupExtractorFactory badLookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(badLookupExtractorFactory.start()).andReturn(false).anyTimes();
    badLookupExtractorFactory.awaitInitialization();
    EasyMock.expectLastCall().andThrow(new TimeoutException());
    EasyMock.expect(badLookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(badLookupExtractorFactory.isInitialized()).andReturn(false).anyTimes();
    EasyMock.replay(badLookupExtractorFactory);
    LookupExtractorFactoryContainer badContainer = new LookupExtractorFactoryContainer("0", badLookupExtractorFactory);
    lookupReferencesManager.add("test", badContainer);

    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));
  }

  @Test
  public void testDropOldContainerAfterNewLoadGoodContainer() throws Exception
  {
    // Test the scenario of dropping the current container only when new container gets initialized
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.replay(lookupExtractorFactory);

    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    LookupExtractorFactory badLookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(badLookupExtractorFactory.start()).andReturn(false).anyTimes();
    badLookupExtractorFactory.awaitInitialization();
    EasyMock.expectLastCall().andThrow(new TimeoutException());
    EasyMock.expect(badLookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(badLookupExtractorFactory.isInitialized()).andReturn(false).anyTimes();
    EasyMock.replay(badLookupExtractorFactory);
    LookupExtractorFactoryContainer badContainer = new LookupExtractorFactoryContainer("0", badLookupExtractorFactory);
    lookupReferencesManager.remove("test", badContainer); // new container to load is badContainer here

    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));
  }

  @Test
  public void testCloseIsCalledAfterStopping() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForCloseIsCalledAfterStopping", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testMock", new LookupExtractorFactoryContainer("0", lookupExtractorFactory));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.stop();
    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testDestroyIsCalledAfterRemove() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);

    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForDestroyIsCalledAfterRemove", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    LookupExtractorFactoryContainer container = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testMock", container);
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.remove("testMock", container);
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testGetNotThere() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForGetNotThere", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("notThere"));
  }

  @Test
  public void testUpdateWithHigherVersion() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory1 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory1.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory1.destroy()).andReturn(true).once();

    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory2.isInitialized()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory2.start()).andReturn(true).once();

    EasyMock.replay(lookupExtractorFactory1, lookupExtractorFactory2);
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForUpdateWithHigherVersion", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("2", lookupExtractorFactory2));
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory1, lookupExtractorFactory2);
  }

  @Test
  public void testUpdateWithLowerVersion() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory1 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory1.start()).andReturn(true).once();

    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);

    EasyMock.replay(lookupExtractorFactory1, lookupExtractorFactory2);
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForUpdateWithLowerVersion", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("0", lookupExtractorFactory2));
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory1, lookupExtractorFactory2);
  }

  @Test
  public void testAddingNewContainerImmediatelyWithoutWaiting() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory1 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory1.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory1.isInitialized()).andReturn(false).anyTimes();
    EasyMock.replay(lookupExtractorFactory1);
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();
    Assert.assertTrue(lookupReferencesManager.get("testName").isPresent());
    EasyMock.verify(lookupExtractorFactory1);
  }

  @Test
  public void testRemoveNonExisting() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForRemoveNonExisting", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.remove("test", null);
    lookupReferencesManager.handlePendingNotices();
  }

  @Test
  public void testGetAllLookupNames() throws Exception
  {
    LookupExtractorFactoryContainer container1 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(ImmutableMap.of("key1", "value1"), true)
    );

    LookupExtractorFactoryContainer container2 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(ImmutableMap.of("key2", "value2"), true)
    );
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("one", container1);
    lookupReferencesManager.add("two", container2);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(ImmutableSet.of("one", "two"), lookupReferencesManager.getAllLookupNames());

    Assert.assertEquals(
        ImmutableSet.of("one", "two"),
        (lookupReferencesManager).getAllLookupNames()
    );
  }

  @Test
  public void testGetCanonicalLookupName()
  {
    String lookupName = "lookupName1";
    Assert.assertEquals(lookupName, lookupReferencesManager.getCanonicalLookupName(lookupName));
  }

  @Test
  public void testGetAllLookupsState() throws Exception
  {
    LookupExtractorFactoryContainer container1 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key1",
                "value1"
            ), true
        )
    );

    LookupExtractorFactoryContainer container2 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key2",
                "value2"
            ), true
        )
    );

    LookupExtractorFactoryContainer container3 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key3",
                "value3"
            ), true
        )
    );
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("one", container1);
    lookupReferencesManager.add("two", container2);
    lookupReferencesManager.handlePendingNotices();
    lookupReferencesManager.remove("one", container1);
    lookupReferencesManager.add("three", container3);

    LookupsState state = lookupReferencesManager.getAllLookupsState();

    Assert.assertEquals(2, state.getCurrent().size());
    Assert.assertEquals(container1, state.getCurrent().get("one"));
    Assert.assertEquals(container2, state.getCurrent().get("two"));

    Assert.assertEquals(1, state.getToLoad().size());
    Assert.assertEquals(container3, state.getToLoad().get("three"));

    Assert.assertEquals(1, state.getToDrop().size());
    Assert.assertTrue(state.getToDrop().contains("one"));
  }

  @Test(timeout = 60_000L)
  public void testRealModeWithMainThread() throws Exception
  {
    LookupReferencesManager lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(temporaryFolder.newFolder().getAbsolutePath()),
        mapper, coordinatorClient, config
    );
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForRealModeWithMainThread", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    Assert.assertTrue(lookupReferencesManager.mainThread.isAlive());

    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);
    lookupReferencesManager.add("test", testContainer);

    while (!Optional.of(testContainer).equals(lookupReferencesManager.get("test"))) {
      Thread.sleep(100);
    }

    Assert.assertEquals(
        ImmutableSet.of("test", "testMockForRealModeWithMainThread"),
        lookupReferencesManager.getAllLookupNames()
    );

    lookupReferencesManager.remove("test", null);

    while (lookupReferencesManager.get("test").isPresent()) {
      Thread.sleep(100);
    }

    Assert.assertEquals(
        ImmutableSet.of("testMockForRealModeWithMainThread"),
        lookupReferencesManager.getAllLookupNames()
    );

    lookupReferencesManager.stop();

    Assert.assertFalse(lookupReferencesManager.mainThread.isAlive());
  }

  @Test
  public void testCoordinatorLookupSync() throws Exception
  {
    LookupExtractorFactoryContainer container1 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key1",
                "value1"
            ), true
        )
    );

    LookupExtractorFactoryContainer container2 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key2",
                "value2"
            ), true
        )
    );

    LookupExtractorFactoryContainer container3 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key3",
                "value3"
            ), true
        )
    );
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testLookup1", container1);
    lookupMap.put("testLookup2", container2);
    lookupMap.put("testLookup3", container3);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);

    lookupReferencesManager.start();
    Assert.assertEquals(Optional.of(container1), lookupReferencesManager.get("testLookup1"));
    Assert.assertEquals(Optional.of(container2), lookupReferencesManager.get("testLookup2"));
    Assert.assertEquals(Optional.of(container3), lookupReferencesManager.get("testLookup3"));

  }

  private Map<String, LookupExtractorFactoryContainer> getLookupMapForSelectiveLoadingOfLookups(LookupLoadingSpec lookupLoadingSpec)
      throws Exception
  {
    LookupExtractorFactoryContainer container1 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(ImmutableMap.of("key1", "value1"), true)
    );

    LookupExtractorFactoryContainer container2 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(ImmutableMap.of("key2", "value2"), true
        )
    );

    LookupExtractorFactoryContainer container3 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(ImmutableMap.of("key3", "value3"), true
        )
    );
    EasyMock.reset(config);
    EasyMock.reset(coordinatorClient);
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testLookup1", container1);
    lookupMap.put("testLookup2", container2);
    lookupMap.put("testLookup3", container3);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    EasyMock.expect(config.getLookupLoadingSpec()).andReturn(lookupLoadingSpec);
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    EasyMock.replay(coordinatorClient);

    lookupReferencesManager.start();
    return lookupMap;
  }

  @Test
  public void testCoordinatorLoadAllLookups() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap = getLookupMapForSelectiveLoadingOfLookups(LookupLoadingSpec.ALL);
    for (String lookupName : lookupMap.keySet()) {
      Assert.assertEquals(Optional.of(lookupMap.get(lookupName)), lookupReferencesManager.get(lookupName));
    }
  }

  @Test
  public void testCoordinatorLoadNoLookups() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap = getLookupMapForSelectiveLoadingOfLookups(LookupLoadingSpec.NONE);
    for (String lookupName : lookupMap.keySet()) {
      Assert.assertFalse(lookupReferencesManager.get(lookupName).isPresent());
    }
  }

  @Test
  public void testCoordinatorLoadSubsetOfLookups() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap =
        getLookupMapForSelectiveLoadingOfLookups(
            LookupLoadingSpec.loadOnly(ImmutableSet.of("testLookup1", "testLookup2"))
        );
    Assert.assertEquals(Optional.of(lookupMap.get("testLookup1")), lookupReferencesManager.get("testLookup1"));
    Assert.assertEquals(Optional.of(lookupMap.get("testLookup2")), lookupReferencesManager.get("testLookup2"));
    Assert.assertFalse(lookupReferencesManager.get("testLookup3").isPresent());
  }

  @Test
  public void testAddWithRequiredLoadingSpec() throws Exception
  {
    LookupLoadingSpec loadingSpec = LookupLoadingSpec.loadOnly(ImmutableSet.of("testLookup1"));
    getLookupMapForSelectiveLoadingOfLookups(loadingSpec);

    LookupExtractorFactoryContainer container2 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(Map.of("key2", "value2"), true
        )
    );
    EasyMock.reset(config);
    EasyMock.expect(config.getLookupLoadingSpec()).andReturn(loadingSpec);
    EasyMock.replay(config);
    lookupReferencesManager.add("testLookup2", container2);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(Set.of("testLookup1"), lookupReferencesManager.getAllLookupNames());
  }

  @Test
  public void testAddWithNoneLoadingSpec() throws Exception
  {
    getLookupMapForSelectiveLoadingOfLookups(LookupLoadingSpec.NONE);

    LookupExtractorFactoryContainer container = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(Map.of("key2", "value2"), true
        )
    );
    EasyMock.reset(config);
    EasyMock.expect(config.getLookupLoadingSpec()).andReturn(LookupLoadingSpec.NONE);
    EasyMock.replay(config);
    lookupReferencesManager.add("testLookup", container);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertTrue(lookupReferencesManager.getAllLookupNames().isEmpty());
  }

  @Test
  public void testLoadLookupOnCoordinatorFailure() throws Exception
  {
    LookupConfig lookupConfig = new LookupConfig(temporaryFolder.newFolder().getAbsolutePath())
    {
      @Override
      public int getCoordinatorRetryDelay()
      {
        return 10;
      }
    };
    lookupReferencesManager = new LookupReferencesManager(
        lookupConfig,
        mapper,
        coordinatorClient,
        config
    );

    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);

    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andThrow(new RuntimeException()).anyTimes();
    EasyMock.replay(coordinatorClient);

    lookupReferencesManager.start();
    lookupReferencesManager.add("testMockForLoadLookupOnCoordinatorFailure", container);
    lookupReferencesManager.handlePendingNotices();
    lookupReferencesManager.stop();
    lookupConfig = new LookupConfig(lookupReferencesManager.lookupSnapshotTaker.getPersistFile(LOOKUP_TIER).getParent())
    {
      @Override
      public int getCoordinatorRetryDelay()
      {
        return 10;
      }
    };

    lookupReferencesManager = new LookupReferencesManager(
        lookupConfig,
        mapper,
        coordinatorClient,
        config,
        true
    );
    EasyMock.reset(config);
    EasyMock.reset(coordinatorClient);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.expect(config.getLookupLoadingSpec()).andReturn(LookupLoadingSpec.ALL).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andThrow(
        new RuntimeException(
            new HttpResponseException(
                new StringFullResponseHolder(
                    new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND),
                    StandardCharsets.UTF_8
                )
            )
        )
    ).anyTimes();
    EasyMock.replay(coordinatorClient);
    lookupReferencesManager.start();
    Assert.assertEquals(
        Optional.of(container),
        lookupReferencesManager.get("testMockForLoadLookupOnCoordinatorFailure")
    );
  }

  @Test
  public void testDisableLookupSync() throws Exception
  {
    LookupConfig lookupConfig = new LookupConfig(null)
    {
      @Override
      public boolean getEnableLookupSyncOnStartup()
      {
        return false;
      }
    };
    LookupReferencesManager lookupReferencesManager = new LookupReferencesManager(
        lookupConfig,
        mapper,
        coordinatorClient,
        config
    );
    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();
    lookupMap.put("testMockForDisableLookupSync", container);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);

    EasyMock.expect(coordinatorClient.fetchLookupsForTierSync(LOOKUP_TIER)).andReturn(lookupMap);
    lookupReferencesManager.start();
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("testMockForDisableLookupSync"));
  }
}
