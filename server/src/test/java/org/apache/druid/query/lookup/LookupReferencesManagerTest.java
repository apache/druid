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
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
  private DruidLeaderClient druidLeaderClient;
  private LookupListeningAnnouncerConfig config;

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);

    config = EasyMock.createMock(LookupListeningAnnouncerConfig.class);

    lookupExtractorFactory = new MapLookupExtractorFactory(
        ImmutableMap.of(
            "key",
            "value"
        ), true
    );
    container = new LookupExtractorFactoryContainer("v0", lookupExtractorFactory);
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    String temporaryPath = temporaryFolder.newFolder().getAbsolutePath();
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(temporaryFolder.newFolder().getAbsolutePath()),
        mapper,
        druidLeaderClient,
        config,
        true
    );
  }

  private static HttpResponse newEmptyResponse(final HttpResponseStatus status)
  {
    final HttpResponse response = EasyMock.createNiceMock(HttpResponse.class);
    EasyMock.expect(response.getStatus()).andReturn(status).anyTimes();
    EasyMock.expect(response.getContent()).andReturn(new BigEndianHeapChannelBuffer(0));
    EasyMock.replay(response);
    return response;
  }

  @Test
  public void testStartStop() throws InterruptedException, IOException
  {
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(null),
        mapper, druidLeaderClient, config
    );

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForStartStop", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
                HttpMethod.GET,
                "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
            ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
                HttpMethod.GET,
                "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
            ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForCloseIsCalledAfterStopping", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForDestroyIsCalledAfterRemove", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForGetNotThere", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForUpdateWithHigherVersion", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForUpdateWithLowerVersion", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
                HttpMethod.GET,
                "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
            ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();
    Assert.assertTrue(lookupReferencesManager.get("testName").isPresent());
    EasyMock.verify(lookupExtractorFactory1);
  }
  @Test
  public void testRemoveNonExisting() throws Exception
  {
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForRemoveNonExisting", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(
        druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true")
    ).andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("one", container1);
    lookupReferencesManager.add("two", container2);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(ImmutableSet.of("one", "two"), lookupReferencesManager.getAllLookupNames());

    Assert.assertEquals(
        ImmutableSet.of("one", "two"),
        ((LookupExtractorFactoryContainerProvider) lookupReferencesManager).getAllLookupNames()
    );
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
    Map<String, Object> lookupMap = new HashMap<>();
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
        mapper, druidLeaderClient, config
    );
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForRealModeWithMainThread", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);
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
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testLookup1", container1);
    lookupMap.put("testLookup2", container2);
    lookupMap.put("testLookup3", container3);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);

    lookupReferencesManager.start();
    Assert.assertEquals(Optional.of(container1), lookupReferencesManager.get("testLookup1"));
    Assert.assertEquals(Optional.of(container2), lookupReferencesManager.get("testLookup2"));
    Assert.assertEquals(Optional.of(container3), lookupReferencesManager.get("testLookup3"));

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
        druidLeaderClient,
        config
    );

    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request)
            .anyTimes();
    EasyMock.expect(druidLeaderClient.go(request)).andThrow(new IllegalStateException()).anyTimes();
    EasyMock.replay(druidLeaderClient);

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
        druidLeaderClient,
        config,
        true
    );
    EasyMock.reset(config);
    EasyMock.reset(druidLeaderClient);
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request)
            .anyTimes();
    EasyMock.expect(druidLeaderClient.go(request)).andThrow(new IllegalStateException()).anyTimes();
    EasyMock.replay(druidLeaderClient);
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
        druidLeaderClient,
        config
    );
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForDisableLookupSync", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    EasyMock.expect(config.getLookupTier()).andReturn(LOOKUP_TIER).anyTimes();
    EasyMock.replay(config);
    EasyMock.expect(druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid/coordinator/v1/lookups/config/lookupTier?detailed=true"
    ))
            .andReturn(request);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        newEmptyResponse(HttpResponseStatus.OK),
        StandardCharsets.UTF_8
    ).addChunk(strResult);
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);

    lookupReferencesManager.start();
    Assert.assertEquals(Optional.empty(), lookupReferencesManager.get("testMockForDisableLookupSync"));
  }
}
