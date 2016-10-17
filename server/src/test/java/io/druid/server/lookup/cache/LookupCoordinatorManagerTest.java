/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.audit.AuditInfo;
import io.druid.common.config.JacksonConfigManager;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.lookup.LookupModule;
import io.druid.server.listener.announcer.ListenerDiscoverer;
import io.druid.server.listener.resource.ListenerResource;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class LookupCoordinatorManagerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final ListenerDiscoverer discoverer = EasyMock.createStrictMock(ListenerDiscoverer.class);
  private final HttpClient client = EasyMock.createStrictMock(HttpClient.class);
  private final JacksonConfigManager configManager = EasyMock.createStrictMock(JacksonConfigManager.class);
  private final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig = new LookupCoordinatorManagerConfig();

  private static final String LOOKUP_TIER = "lookup_tier";
  private static final String SINGLE_LOOKUP_NAME = "lookupName";
  private static final Map<String, Object> SINGLE_LOOKUP_SPEC = ImmutableMap.<String, Object>of(
      "some property",
      "some value"
  );
  private static final Map<String, Map<String, Object>> SINGLE_LOOKUP_MAP = ImmutableMap.<String, Map<String, Object>>of(
      SINGLE_LOOKUP_NAME,
      SINGLE_LOOKUP_SPEC
  );
  private static final Map<String, Map<String, Map<String, Object>>> TIERED_LOOKUP_MAP = (Map<String, Map<String, Map<String, Object>>>) ImmutableMap.<String, Map<String, Map<String, Object>>>of(
      LOOKUP_TIER,
      SINGLE_LOOKUP_MAP
  );
  private static final Map<String, Map<String, Map<String, Object>>> EMPTY_TIERED_LOOKUP = (Map<String, Map<String, Map<String, Object>>>) ImmutableMap.<String, Map<String, Map<String, Object>>>of();
  private static final AtomicLong EVENT_EMITS = new AtomicLong(0L);
  private static ServiceEmitter SERVICE_EMITTER;

  @BeforeClass
  public static void setUpStatic()
  {
    LoggingEmitter loggingEmitter = EasyMock.createNiceMock(LoggingEmitter.class);
    EasyMock.replay(loggingEmitter);
    SERVICE_EMITTER = new ServiceEmitter("", "", loggingEmitter)
    {
      @Override
      public void emit(Event event)
      {
        EVENT_EMITS.incrementAndGet();
        super.emit(event);
      }
    };
    com.metamx.emitter.EmittingLogger.registerEmitter(SERVICE_EMITTER);
  }

  @Before
  public void setUp() throws IOException
  {
    SERVICE_EMITTER.flush();
    EVENT_EMITS.set(0L);
  }

  @After
  public void tearDown() throws IOException
  {
    SERVICE_EMITTER.flush();
    Assert.assertEquals(0, EVENT_EMITS.get());
  }

  @Test
  public void testUpdateAllOnHost() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final URL url = LookupCoordinatorManager.getLookupsURL(HostAndPort.fromString("localhost"));
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(ImmutableMap.of(
        "status",
        "accepted",
        LookupModule.FAILED_UPDATES_KEY,
        ImmutableMap.of()
    )))));
    EasyMock.expect(client.go(
        EasyMock.<Request>anyObject(),
        EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
        EasyMock.<Duration>anyObject()
    )).andReturn(future).once();

    EasyMock.replay(client, responseHandler);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(200);
        reasonString.set("");
        return responseHandler;
      }
    };
    manager.updateAllOnHost(
        url,
        SINGLE_LOOKUP_MAP
    );

    EasyMock.verify(client, responseHandler);
  }

  @Test
  public void testUpdateAllOnHostFailsWithFailedThings() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final String failedLookup = "failedLookup";
    final URL url = LookupCoordinatorManager.getLookupsURL(HostAndPort.fromString("localhost"));
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(ImmutableMap.of(
        "status",
        "accepted",
        LookupModule.FAILED_UPDATES_KEY,
        ImmutableMap.of(
            failedLookup,
            ImmutableMap.of()
        )
    )))));
    EasyMock.expect(client.go(
        EasyMock.<Request>anyObject(),
        EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
        EasyMock.<Duration>anyObject()
    )).andReturn(future).once();

    EasyMock.replay(client, responseHandler);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(200);
        reasonString.set("");
        return responseHandler;
      }
    };
    expectedException.expectMessage("Lookups failed to update: [\"" + failedLookup + "\"]");
    try {
      manager.updateAllOnHost(
          url,
          SINGLE_LOOKUP_MAP
      );
    }
    finally {

      EasyMock.verify(client, responseHandler);
    }
  }

  @Test
  public void testUpdateAllOnHostFailsWhenServerReturnsWeird() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final String failedLookup = "failedLookup";
    final URL url = LookupCoordinatorManager.getLookupsURL(HostAndPort.fromString("localhost"));
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(ImmutableMap.of(
        "status",
        "accepted"
    )))));
    EasyMock.expect(client.go(
        EasyMock.<Request>anyObject(),
        EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
        EasyMock.<Duration>anyObject()
    )).andReturn(future).once();

    EasyMock.replay(client, responseHandler);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(200);
        reasonString.set("");
        return responseHandler;
      }
    };
    expectedException.expectMessage(String.format(
        "Update result did not have field for [%s]",
        LookupModule.FAILED_UPDATES_KEY
    ));
    try {
      manager.updateAllOnHost(
          url,
          SINGLE_LOOKUP_MAP
      );
    }
    finally {

      EasyMock.verify(client, responseHandler);
    }
  }


  @Test
  public void testUpdateAllOnHostException() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final URL url = LookupCoordinatorManager.getLookupsURL(HostAndPort.fromString("localhost"));
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(new byte[0]));
    EasyMock.expect(client.go(
        EasyMock.<Request>anyObject(),
        EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
        EasyMock.<Duration>anyObject()
    )).andReturn(future).once();

    EasyMock.replay(client, responseHandler);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(500);
        reasonString.set("");
        return responseHandler;
      }
    };
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof IOException && ((IOException) o).getMessage().startsWith("Bad update request");
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      manager.updateAllOnHost(
          url,
          SINGLE_LOOKUP_MAP
      );
    }
    finally {
      EasyMock.verify(client, responseHandler);
    }
  }

  @Test
  public void testParseErrorUpdateAllOnHost() throws Exception
  {

    final AtomicReference<List<Map<String, Object>>> configVal = new AtomicReference<>(null);

    final URL url = LookupCoordinatorManager.getLookupsURL(HostAndPort.fromString("localhost"));

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(EasyMock.anyString(), EasyMock.<TypeReference>anyObject()))
            .andReturn(configVal);

    final JsonProcessingException ex = EasyMock.createStrictMock(JsonProcessingException.class);

    final ObjectMapper mapper = EasyMock.createStrictMock(ObjectMapper.class);
    EasyMock.expect(mapper.writeValueAsBytes(EasyMock.eq(SINGLE_LOOKUP_MAP))).andThrow(ex);

    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return ex == o;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });

    EasyMock.replay(mapper);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    );
    try {
      manager.updateAllOnHost(
          url,
          SINGLE_LOOKUP_MAP
      );
    }
    finally {
      EasyMock.verify(mapper);
    }
  }


  @Test
  public void testUpdateAll() throws Exception
  {
    final List<URL> urls = ImmutableList.of(new URL("http://foo.bar"));
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      Collection<URL> getAllHostsAnnounceEndpoint(String tier)
      {
        return urls;
      }

      @Override
      void updateAllOnHost(final URL url, Map<String, Map<String, Object>> updatedLookups)
          throws IOException, InterruptedException, ExecutionException
      {
        if (!urls.get(0).equals(url) || updatedLookups != SINGLE_LOOKUP_MAP) {
          throw new RuntimeException("Not matched");
        }
      }
    };
    // Should be no-ops
    manager.updateAllOnTier(null, null);
  }


  @Test
  public void testUpdateAllIOException() throws Exception
  {
    final IOException ex = new IOException("test exception");
    final List<URL> urls = ImmutableList.of(new URL("http://foo.bar"));
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      Collection<URL> getAllHostsAnnounceEndpoint(String Tier)
      {
        return urls;
      }

      @Override
      void updateAllOnHost(final URL url, Map<String, Map<String, Object>> updatedLookups)
          throws IOException, InterruptedException, ExecutionException
      {
        throw ex;
      }
    };
    // Should log and pass io exception
    manager.updateAllOnTier(LOOKUP_TIER, SINGLE_LOOKUP_MAP);
    SERVICE_EMITTER.flush();
    Assert.assertEquals(1, EVENT_EMITS.get());
    EVENT_EMITS.set(0);
  }

  @Test
  public void testUpdateAllInterrupted() throws Exception
  {
    final InterruptedException ex = new InterruptedException("interruption test");
    final List<URL> urls = ImmutableList.of(new URL("http://foo.bar"));
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      Collection<URL> getAllHostsAnnounceEndpoint(String tier)
      {
        return urls;
      }

      @Override
      void updateAllOnHost(final URL url, Map<String, Map<String, Object>> knownLookups)
          throws IOException, InterruptedException, ExecutionException
      {
        throw ex;
      }
    };
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        if (!(o instanceof RuntimeException)) {
          return false;
        }
        final Throwable e = (Throwable) o;
        return e.getCause() == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      manager.updateAllOnTier(LOOKUP_TIER, SINGLE_LOOKUP_MAP);
    }
    finally {
      // Clear status
      Thread.interrupted();
    }
  }

  @Test
  public void testGetAllHostsAnnounceEndpoint() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    );

    EasyMock.expect(discoverer.getNodes(EasyMock.eq(LookupModule.getTierListenerPath(LOOKUP_TIER))))
            .andReturn(ImmutableList.<HostAndPort>of())
            .once();
    EasyMock.replay(discoverer);
    Assert.assertEquals(ImmutableList.of(), manager.getAllHostsAnnounceEndpoint(LOOKUP_TIER));
    EasyMock.verify(discoverer);
    EasyMock.reset(discoverer);

    EasyMock.expect(discoverer.getNodes(EasyMock.eq(LookupModule.getTierListenerPath(LOOKUP_TIER))))
            .andReturn(Collections.<HostAndPort>singletonList(null))
            .once();
    EasyMock.replay(discoverer);
    Assert.assertEquals(ImmutableList.of(), manager.getAllHostsAnnounceEndpoint(LOOKUP_TIER));
    EasyMock.verify(discoverer);
  }

  @Test
  public void testGetLookupURL() throws Exception
  {
    final String path = ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY;
    Assert.assertEquals(
        new URL("http", "someHost", 1, path),
        LookupCoordinatorManager.getLookupsURL(
            HostAndPort.fromParts("someHost", 1)
        )
    );
    Assert.assertEquals(
        new URL("http", "someHost", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            HostAndPort.fromString("someHost")
        )
    );

    Assert.assertEquals(
        new URL("http", "::1", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            HostAndPort.fromString("::1")
        )
    );
    Assert.assertEquals(
        new URL("http", "[::1]", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            HostAndPort.fromString("[::1]")
        )
    );
    Assert.assertEquals(
        new URL("http", "::1", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            HostAndPort.fromString("::1")
        )
    );
  }

  @Test
  public void testUpdateLookupAdds() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return EMPTY_TIERED_LOOKUP;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(TIERED_LOOKUP_MAP),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    manager.updateLookup(LOOKUP_TIER, SINGLE_LOOKUP_NAME, SINGLE_LOOKUP_SPEC, auditInfo);
    EasyMock.verify(configManager);
  }


  @Test
  public void testUpdateLookupFailsUnitialized() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return null;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    expectedException.expect(ISE.class);
    manager.updateLookups(TIERED_LOOKUP_MAP, auditInfo);
  }

  @Test
  public void testUpdateLookupUpdates() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("prop", "old");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(LOOKUP_TIER, ImmutableMap.of(
            "foo", ImmutableMap.<String, Object>of("prop", "old"),
            "ignore", ignore
        ));
      }
    };
    final Map<String, Object> newSpec = ImmutableMap.<String, Object>of(
        "prop",
        "new"
    );
    final Map<String, Map<String, Object>> lookup = ImmutableMap.<String, Map<String, Object>>of(
        "foo", newSpec,
        "ignore", ignore
    );
    final Map<String, Map<String, Map<String, Object>>> tier = ImmutableMap.of(
        LOOKUP_TIER,
        lookup
    );
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(ImmutableMap.of(LOOKUP_TIER, ImmutableMap.of(
            "foo", newSpec,
            "ignore", ignore
        ))),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    manager.updateLookups(tier, auditInfo);
    EasyMock.verify(configManager);
  }


  @Test
  public void testUpdateLookupFailsBadUpdates() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("prop", "old");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(LOOKUP_TIER, ImmutableMap.of(
            "foo", ImmutableMap.<String, Object>of("prop", "old"),
            "ignore", ignore
        ));
      }
    };
    final Map<String, Object> newSpec = ImmutableMap.<String, Object>of(
        "prop",
        "new"
    );
    final Map<String, Map<String, Object>> lookup = ImmutableMap.<String, Map<String, Object>>of(
        "foo", newSpec,
        "ignore", ignore
    );
    final Map<String, Map<String, Map<String, Object>>> tier = ImmutableMap.of(
        LOOKUP_TIER,
        lookup
    );
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(ImmutableMap.of(LOOKUP_TIER, ImmutableMap.of(
            "foo", newSpec,
            "ignore", ignore
        ))),
        EasyMock.eq(auditInfo)
    )).andReturn(false).once();
    EasyMock.replay(configManager);
    Assert.assertFalse(manager.updateLookups(tier, auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testUpdateLookupsOnlyAddsToTier() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("prop", "old");
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(
            LOOKUP_TIER + "1",
            ImmutableMap.<String, Map<String, Object>>of("foo", ImmutableMap.<String, Object>of("prop", "old")),
            LOOKUP_TIER + "2",
            ImmutableMap.of("ignore", ignore)
        );
      }
    };
    final Map<String, Object> newSpec = ImmutableMap.<String, Object>of(
        "prop",
        "new"
    );
    EasyMock.reset(configManager);
    EasyMock.expect(
        configManager.set(
            EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
            EasyMock.eq(ImmutableMap.<String, Map<String, Map<String, Object>>>of(
                LOOKUP_TIER + "1", ImmutableMap.of("foo", newSpec),
                LOOKUP_TIER + "2", ImmutableMap.of("ignore", ignore)
            )),
            EasyMock.eq(auditInfo)
        )
    ).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.updateLookups(ImmutableMap.<String, Map<String, Map<String, Object>>>of(
        LOOKUP_TIER + "1", ImmutableMap.<String, Map<String, Object>>of(
            "foo",
            newSpec
        )
    ), auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testUpdateLookupsAddsNewTier() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("prop", "old");
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(
            LOOKUP_TIER + "2",
            ImmutableMap.of("ignore", ignore)
        );
      }
    };
    final Map<String, Object> newSpec = ImmutableMap.<String, Object>of(
        "prop",
        "new"
    );
    EasyMock.reset(configManager);
    EasyMock.expect(
        configManager.set(
            EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
            EasyMock.eq(ImmutableMap.<String, Map<String, Map<String, Object>>>of(
                LOOKUP_TIER + "1", ImmutableMap.of("foo", newSpec),
                LOOKUP_TIER + "2", ImmutableMap.of("ignore", ignore)
            )),
            EasyMock.eq(auditInfo)
        )
    ).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.updateLookups(ImmutableMap.<String, Map<String, Map<String, Object>>>of(
        LOOKUP_TIER + "1", ImmutableMap.<String, Map<String, Object>>of(
            "foo",
            newSpec
        )
    ), auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testUpdateLookupsAddsNewLookup() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("prop", "old");
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(
            LOOKUP_TIER + "1",
            ImmutableMap.<String, Map<String, Object>>of("foo1", ImmutableMap.<String, Object>of("prop", "old")),
            LOOKUP_TIER + "2",
            ImmutableMap.of("ignore", ignore)
        );
      }
    };
    final Map<String, Object> newSpec = ImmutableMap.<String, Object>of(
        "prop",
        "new"
    );
    EasyMock.reset(configManager);
    EasyMock.expect(
        configManager.set(
            EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
            EasyMock.eq(ImmutableMap.<String, Map<String, Map<String, Object>>>of(
                LOOKUP_TIER + "1", ImmutableMap.of(
                    "foo1", ImmutableMap.<String, Object>of("prop", "old"),
                    "foo2", newSpec
                ),
                LOOKUP_TIER + "2", ImmutableMap.of("ignore", ignore)
            )),
            EasyMock.eq(auditInfo)
        )
    ).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.updateLookups(ImmutableMap.<String, Map<String, Map<String, Object>>>of(
        LOOKUP_TIER + "1", ImmutableMap.<String, Map<String, Object>>of(
            "foo2",
            newSpec
        )
    ), auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testDeleteLookup() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("lookup", "ignore");
    final Map<String, Object> lookup = ImmutableMap.<String, Object>of("lookup", "foo");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(LOOKUP_TIER, ImmutableMap.of(
            "foo", lookup,
            "ignore", ignore
        ));
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(ImmutableMap.of(LOOKUP_TIER, ImmutableMap.of(
            "ignore", ignore
        ))),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.deleteLookup(LOOKUP_TIER, "foo", auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testDeleteLookupIgnoresMissing() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("lookup", "ignore");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(LOOKUP_TIER, ImmutableMap.of(
            "ignore", ignore
        ));
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    Assert.assertFalse(manager.deleteLookup(LOOKUP_TIER, "foo", auditInfo));
  }

  @Test
  public void testDeleteLookupIgnoresNotReady() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return null;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    Assert.assertFalse(manager.deleteLookup(LOOKUP_TIER, "foo", auditInfo));
  }

  @Test
  public void testDeleteAllTier() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {

      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(200);
        reasonString.set("");
        return responseHandler;
      }
    };
    final HostAndPort hostAndPort = HostAndPort.fromParts("someHost", 8080);
    final Collection<String> drop = ImmutableList.of("lookup1");
    EasyMock
        .expect(discoverer.getNodes(LookupModule.getTierListenerPath(LOOKUP_TIER)))
        .andReturn(ImmutableList.of(hostAndPort))
        .once();
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(new byte[0]));
    EasyMock
        .expect(client.go(
            EasyMock.<Request>anyObject(),
            EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
            EasyMock.<Duration>anyObject()
        ))
        .andReturn(future)
        .once();
    EasyMock.replay(client, discoverer, responseHandler);
    manager.deleteAllOnTier(LOOKUP_TIER, drop);
    EasyMock.verify(client, discoverer, responseHandler);
  }

  @Test
  public void testDeleteAllTierMissing() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(404);
        reasonString.set("");
        return responseHandler;
      }
    };
    final HostAndPort hostAndPort = HostAndPort.fromParts("someHost", 8080);
    final Collection<String> drop = ImmutableList.of("lookup1");
    EasyMock
        .expect(discoverer.getNodes(LookupModule.getTierListenerPath(LOOKUP_TIER)))
        .andReturn(ImmutableList.of(hostAndPort))
        .once();
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(new byte[0]));
    EasyMock
        .expect(client.go(
            EasyMock.<Request>anyObject(),
            EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
            EasyMock.<Duration>anyObject()
        ))
        .andReturn(future)
        .once();
    EasyMock.replay(client, discoverer, responseHandler);
    manager.deleteAllOnTier(LOOKUP_TIER, drop);
    EasyMock.verify(client, discoverer, responseHandler);
  }

  @Test
  public void testDeleteAllTierContinuesOnMissing() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);
    final AtomicInteger responseHandlerCalls = new AtomicInteger(0);
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        if (responseHandlerCalls.getAndIncrement() == 0) {
          returnCode.set(404);
          reasonString.set("Not Found");
        } else {
          returnCode.set(202);
          reasonString.set("");
        }
        return responseHandler;
      }
    };
    final HostAndPort hostAndPort = HostAndPort.fromParts("someHost", 8080);
    final Collection<String> drop = ImmutableList.of("lookup1");
    EasyMock
        .expect(discoverer.getNodes(LookupModule.getTierListenerPath(LOOKUP_TIER)))
        .andReturn(ImmutableList.of(hostAndPort, hostAndPort))
        .once();
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(new byte[0]));
    EasyMock
        .expect(client.go(
            EasyMock.<Request>anyObject(),
            EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
            EasyMock.<Duration>anyObject()
        ))
        .andReturn(future)
        .times(2);
    EasyMock.replay(client, discoverer, responseHandler);
    manager.deleteAllOnTier(LOOKUP_TIER, drop);
    EasyMock.verify(client, discoverer, responseHandler);
    Assert.assertEquals(2, responseHandlerCalls.get());
  }

  @Test
  public void testGetLookup() throws Exception
  {
    final Map<String, Object> lookup = ImmutableMap.<String, Object>of("lookup", "foo");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(LOOKUP_TIER, ImmutableMap.of(
            "foo",
            lookup
        ));
      }
    };
    Assert.assertEquals(lookup, manager.getLookup(LOOKUP_TIER, "foo"));
    Assert.assertNull(manager.getLookup(LOOKUP_TIER, "does not exit"));
    Assert.assertNull(manager.getLookup("not a tier", "foo"));
  }


  @Test
  public void testGetLookupIgnoresMalformed() throws Exception
  {
    final Map<String, Object> lookup = ImmutableMap.<String, Object>of("lookup", "foo");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Map<String, Object>>>of(LOOKUP_TIER, ImmutableMap.of(
            "foo", lookup,
            "bar", ImmutableMap.<String, Object>of()
        ));
      }
    };
    Assert.assertEquals(lookup, manager.getLookup(LOOKUP_TIER, "foo"));
    Assert.assertNull(manager.getLookup(LOOKUP_TIER, "does not exit"));
    Assert.assertNull(manager.getLookup("not a tier", "foo"));
  }

  @Test
  public void testGetLookupIgnoresNotReady() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
      {
        return null;
      }
    };
    Assert.assertNull(manager.getLookup(LOOKUP_TIER, "foo"));
  }

  @Test
  public void testStart() throws Exception
  {
    final AtomicReference<List<Map<String, Object>>> lookupRef = new AtomicReference<>(null);

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.<TypeReference>anyObject(),
        EasyMock.<AtomicReference>isNull()
    )).andReturn(lookupRef).once();
    EasyMock.replay(configManager);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        new LookupCoordinatorManagerConfig(){
          @Override
          public long getPeriod(){
            return 1;
          }
        }
    );
    manager.start();
    manager.start();
    Assert.assertTrue(manager.backgroundManagerIsRunning());
    Assert.assertNull(manager.getKnownLookups());
    Assert.assertFalse(manager.waitForBackgroundTermination(10));
    EasyMock.verify(configManager);
  }

  @Test
  public void testStop() throws Exception
  {
    final AtomicReference<List<Map<String, Object>>> lookupRef = new AtomicReference<>(null);

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.<TypeReference>anyObject(),
        EasyMock.<AtomicReference>isNull()
    )).andReturn(lookupRef).once();
    EasyMock.replay(configManager);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    );
    manager.start();
    Assert.assertTrue(manager.backgroundManagerIsRunning());
    Assert.assertFalse(manager.waitForBackgroundTermination(10));
    manager.stop();
    manager.stop();
    Assert.assertTrue(manager.waitForBackgroundTermination(10));
    Assert.assertFalse(manager.backgroundManagerIsRunning());
    EasyMock.verify(configManager);
  }

  @Test
  public void testStartTooMuch() throws Exception
  {
    final AtomicReference<List<Map<String, Object>>> lookupRef = new AtomicReference<>(null);

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.<TypeReference>anyObject(),
        EasyMock.<AtomicReference>isNull()
    )).andReturn(lookupRef).once();
    EasyMock.replay(configManager);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    );
    manager.start();
    Assert.assertTrue(manager.backgroundManagerIsRunning());
    Assert.assertFalse(manager.waitForBackgroundTermination(10));
    manager.stop();
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof ISE && ((ISE) o).getMessage().equals("Cannot restart after stop!");
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      manager.start();
    }
    finally {
      EasyMock.verify(configManager);
    }
  }

  @Test
  public void testLookupDiscoverAll() throws Exception
  {
    final List<String> fakeChildren = ImmutableList.of("tier1", "tier2");
    EasyMock.reset(discoverer);
    EasyMock.expect(discoverer.discoverChildren(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY))
            .andReturn(fakeChildren)
            .once();
    EasyMock.replay(discoverer);
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    );
    Assert.assertEquals(fakeChildren, manager.discoverTiers());
    EasyMock.verify(discoverer);
  }


  @Test
  public void testLookupDiscoverAllExceptional() throws Exception
  {
    final IOException ex = new IOException("some exception");
    EasyMock.reset(discoverer);
    EasyMock.expect(discoverer.discoverChildren(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY))
            .andThrow(ex)
            .once();
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    EasyMock.replay(discoverer);
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    );
    try {
      manager.discoverTiers();
    }
    finally {
      EasyMock.verify(discoverer);
    }
  }
}
