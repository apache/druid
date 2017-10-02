/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.discovery.DruidLeaderClient;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

public class LookupReferencesManagerTest
{
  LookupReferencesManager lookupReferencesManager;

  private DruidLeaderClient druidLeaderClient;

  private LookupListeningAnnouncerConfig config;

  private static final String propertyBase = "some.property";

  private static final String LOOKUP_TIER = "lookupTier";

  private static final int LOOKUP_THREADS = 1;

  private static final boolean LOOKUP_DISABLE = false;

  LookupExtractorFactory lookupExtractorFactory;

  LookupExtractorFactoryContainer container;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);

    config = createMock(LookupListeningAnnouncerConfig.class);

    lookupExtractorFactory = new MapLookupExtractorFactory(
        ImmutableMap.<String, String>of(
            "key",
            "value"
        ), true
    );
    container = new LookupExtractorFactoryContainer("v0", lookupExtractorFactory);
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    String temporaryPath = temporaryFolder.newFolder().getAbsolutePath();
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(temporaryFolder.newFolder().getAbsolutePath()),
        mapper, druidLeaderClient, config,
        true
    );
  }

  @Test
  public void testStartStop() throws JsonProcessingException, InterruptedException, MalformedURLException
  {
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(null),
        mapper, druidLeaderClient, config
    );

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForStartStop", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
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
    lookupReferencesManager.remove("test");
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
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForAddGetRemove", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    Assert.assertNull(lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(testContainer, lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test");
    lookupReferencesManager.handlePendingNotices();

    Assert.assertNull(lookupReferencesManager.get("test"));
  }

  @Test
  public void testCloseIsCalledAfterStopping() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForCloseIsCalledAfterStopping", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testMock", new LookupExtractorFactoryContainer("0", lookupExtractorFactory));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.stop();
    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testCloseIsCalledAfterRemove() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);

    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForCloseIsCalledAfterRemove", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testMock", new LookupExtractorFactoryContainer("0", lookupExtractorFactory));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.remove("testMock");
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
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    Assert.assertNull(lookupReferencesManager.get("notThere"));
  }

  @Test
  public void testUpdateWithHigherVersion() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory1 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory1.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory1.close()).andReturn(true).once();

    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory2.start()).andReturn(true).once();

    EasyMock.replay(lookupExtractorFactory1, lookupExtractorFactory2);
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForUpdateWithHigherVersion", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
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
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("0", lookupExtractorFactory2));
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory1, lookupExtractorFactory2);
  }

  @Test
  public void testRemoveNonExisting() throws Exception
  {
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForRemoveNonExisting", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.remove("test");
    lookupReferencesManager.handlePendingNotices();
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
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    lookupReferencesManager.add("one", container1);
    lookupReferencesManager.add("two", container2);
    lookupReferencesManager.handlePendingNotices();
    lookupReferencesManager.remove("one");
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

  @Test(timeout = 20000)
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
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    Assert.assertTrue(lookupReferencesManager.mainThread.isAlive());

    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertNull(lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);
    lookupReferencesManager.add("test", testContainer);

    while (!testContainer.equals(lookupReferencesManager.get("test"))) {
      Thread.sleep(100);
    }

    lookupReferencesManager.remove("test");

    while (lookupReferencesManager.get("test") != null) {
      Thread.sleep(100);
    }

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
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);

    lookupReferencesManager.start();
    Assert.assertEquals(container1, lookupReferencesManager.get("testLookup1"));
    Assert.assertEquals(container2, lookupReferencesManager.get("testLookup2"));
    Assert.assertEquals(container3, lookupReferencesManager.get("testLookup3"));

  }

  @Test
  public void testLoadLookupOnCoordinatorFailure() throws Exception
  {
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForLoadLookupOnCoordinatorFailure", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.NOT_FOUND,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andThrow(new IllegalStateException());
    replay(druidLeaderClient);

    lookupReferencesManager.start();
    lookupReferencesManager.add("testMockForLoadLookupOnCoordinatorFailure", container);
    lookupReferencesManager.handlePendingNotices();
    lookupReferencesManager.stop();
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(lookupReferencesManager.lookupSnapshotTaker.getPersistFile().getParent()),
        mapper, druidLeaderClient, config,
        true
    );
    reset(config);
    reset(druidLeaderClient);
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    expect(druidLeaderClient.go(request)).andThrow(new IllegalStateException());
    replay(druidLeaderClient);
    lookupReferencesManager.start();
    Assert.assertEquals(container, lookupReferencesManager.get("testMockForLoadLookupOnCoordinatorFailure"));
  }

  @Test
  public void testDisableLookupSync() throws Exception
  {
    LookupReferencesManager lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(null),
        mapper, druidLeaderClient, config
    );
    Map<String, Object> lookupMap = new HashMap<>();
    lookupMap.put("testMockForDisableLookupSync", container);
    String strResult = mapper.writeValueAsString(lookupMap);
    Request request = new Request(HttpMethod.GET, new URL("http://localhost:1234/xx"));
    expect(config.getLookupTier()).andReturn(LOOKUP_TIER);
    replay(config);
    expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/lookupTier?detailed=true"))
        .andReturn(request);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);

    lookupReferencesManager.start();
    Assert.assertNull(lookupReferencesManager.get("testMockForDisableLookupSync"));
  }

}
