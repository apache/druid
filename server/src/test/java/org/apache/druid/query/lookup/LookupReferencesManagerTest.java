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
import org.apache.druid.testing.TemporaryFolderExtension;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

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

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = new TemporaryFolderExtension();
  LookupReferencesManager lookupReferencesManager;
  LookupExtractorFactory lookupExtractorFactory;
  LookupExtractorFactoryContainer container;
  ObjectMapper mapper = new DefaultObjectMapper();
  private CoordinatorClientImpl coordinatorClient;
  private LookupListeningAnnouncerConfig config;

  @BeforeEach
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
    Assertions.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assertions.assertNull(lookupReferencesManager.mainThread);
    Assertions.assertNull(lookupReferencesManager.stateRef.get());

    lookupReferencesManager.start();
    Assertions.assertTrue(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assertions.assertTrue(lookupReferencesManager.mainThread.isAlive());
    Assertions.assertNotNull(lookupReferencesManager.stateRef.get());

    lookupReferencesManager.stop();
    Assertions.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assertions.assertFalse(lookupReferencesManager.mainThread.isAlive());
  }

  @Test
  public void testGetExceptionWhenClosed()
  {
    Assertions.assertThrows(IllegalStateException.class, () ->
      lookupReferencesManager.get("test"));
  }

  @Test
  public void testAddExceptionWhenClosed()
  {
    Assertions.assertThrows(IllegalStateException.class, () ->
      lookupReferencesManager.add("test", EasyMock.createMock(LookupExtractorFactoryContainer.class)));
  }

  @Test
  public void testRemoveExceptionWhenClosed()
  {
    Assertions.assertThrows(IllegalStateException.class, () ->
      lookupReferencesManager.remove("test", null));
  }

  @Test
  public void testGetAllLookupsStateExceptionWhenClosed()
  {
    Assertions.assertThrows(IllegalStateException.class, () ->
      lookupReferencesManager.getAllLookupsState());
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
    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assertions.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));
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
    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assertions.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

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

    Assertions.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));
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
    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assertions.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

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

    Assertions.assertEquals(Optional.of(testContainer), lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));
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
    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("notThere"));
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
    Assertions.assertTrue(lookupReferencesManager.get("testName").isPresent());
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

    Assertions.assertEquals(ImmutableSet.of("one", "two"), lookupReferencesManager.getAllLookupNames());

    Assertions.assertEquals(
        ImmutableSet.of("one", "two"),
        (lookupReferencesManager).getAllLookupNames()
    );
  }

  @Test
  public void testGetCanonicalLookupName()
  {
    String lookupName = "lookupName1";
    Assertions.assertEquals(lookupName, lookupReferencesManager.getCanonicalLookupName(lookupName));
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

    Assertions.assertEquals(2, state.getCurrent().size());
    Assertions.assertEquals(container1, state.getCurrent().get("one"));
    Assertions.assertEquals(container2, state.getCurrent().get("two"));

    Assertions.assertEquals(1, state.getToLoad().size());
    Assertions.assertEquals(container3, state.getToLoad().get("three"));

    Assertions.assertEquals(1, state.getToDrop().size());
    Assertions.assertTrue(state.getToDrop().contains("one"));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
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
    Assertions.assertTrue(lookupReferencesManager.mainThread.isAlive());

    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.destroy()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.isInitialized()).andReturn(true).anyTimes();
    EasyMock.replay(lookupExtractorFactory);
    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);
    lookupReferencesManager.add("test", testContainer);

    while (!Optional.of(testContainer).equals(lookupReferencesManager.get("test"))) {
      Thread.sleep(100);
    }

    Assertions.assertEquals(
        ImmutableSet.of("test", "testMockForRealModeWithMainThread"),
        lookupReferencesManager.getAllLookupNames()
    );

    lookupReferencesManager.remove("test", null);

    while (lookupReferencesManager.get("test").isPresent()) {
      Thread.sleep(100);
    }

    Assertions.assertEquals(
        ImmutableSet.of("testMockForRealModeWithMainThread"),
        lookupReferencesManager.getAllLookupNames()
    );

    lookupReferencesManager.stop();

    Assertions.assertFalse(lookupReferencesManager.mainThread.isAlive());
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
    Assertions.assertEquals(Optional.of(container1), lookupReferencesManager.get("testLookup1"));
    Assertions.assertEquals(Optional.of(container2), lookupReferencesManager.get("testLookup2"));
    Assertions.assertEquals(Optional.of(container3), lookupReferencesManager.get("testLookup3"));

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
      Assertions.assertEquals(Optional.of(lookupMap.get(lookupName)), lookupReferencesManager.get(lookupName));
    }
  }

  @Test
  public void testCoordinatorLoadNoLookups() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap = getLookupMapForSelectiveLoadingOfLookups(LookupLoadingSpec.NONE);
    for (String lookupName : lookupMap.keySet()) {
      Assertions.assertFalse(lookupReferencesManager.get(lookupName).isPresent());
    }
  }

  @Test
  public void testCoordinatorLoadSubsetOfLookups() throws Exception
  {
    Map<String, LookupExtractorFactoryContainer> lookupMap =
        getLookupMapForSelectiveLoadingOfLookups(
            LookupLoadingSpec.loadOnly(ImmutableSet.of("testLookup1", "testLookup2"))
        );
    Assertions.assertEquals(Optional.of(lookupMap.get("testLookup1")), lookupReferencesManager.get("testLookup1"));
    Assertions.assertEquals(Optional.of(lookupMap.get("testLookup2")), lookupReferencesManager.get("testLookup2"));
    Assertions.assertFalse(lookupReferencesManager.get("testLookup3").isPresent());
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

    Assertions.assertEquals(Set.of("testLookup1"), lookupReferencesManager.getAllLookupNames());
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

    Assertions.assertTrue(lookupReferencesManager.getAllLookupNames().isEmpty());
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
    Assertions.assertEquals(
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
    Assertions.assertEquals(Optional.empty(), lookupReferencesManager.get("testMockForDisableLookupSync"));
  }
}
