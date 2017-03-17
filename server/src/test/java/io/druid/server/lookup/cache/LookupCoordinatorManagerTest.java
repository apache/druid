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

package io.druid.server.lookup.cache;

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
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.server.listener.announcer.ListenerDiscoverer;
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

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private static final LookupExtractorFactoryMapContainer SINGLE_LOOKUP_SPEC_V0 =
      new LookupExtractorFactoryMapContainer(
          "v0",
          ImmutableMap.<String, Object>of("k0", "v0")
  );
  private static final LookupExtractorFactoryMapContainer SINGLE_LOOKUP_SPEC_V1 =
      new LookupExtractorFactoryMapContainer(
          "v1",
          ImmutableMap.<String, Object>of("k1", "v1")
      );
  private static final Map<String, LookupExtractorFactoryMapContainer> SINGLE_LOOKUP_MAP_V0 = ImmutableMap.of(
      SINGLE_LOOKUP_NAME,
      SINGLE_LOOKUP_SPEC_V0
  );
  private static final Map<String, LookupExtractorFactoryMapContainer> SINGLE_LOOKUP_MAP_V1 = ImmutableMap.of(
      SINGLE_LOOKUP_NAME,
      SINGLE_LOOKUP_SPEC_V1
  );
  private static final Map<String, Map<String, LookupExtractorFactoryMapContainer>> TIERED_LOOKUP_MAP_V0 = ImmutableMap.of(
      LOOKUP_TIER,
      SINGLE_LOOKUP_MAP_V0
  );
  private static final Map<String, Map<String, LookupExtractorFactoryMapContainer>> TIERED_LOOKUP_MAP_V1 = ImmutableMap.of(
      LOOKUP_TIER,
      SINGLE_LOOKUP_MAP_V1
  );
  private static final Map<String, Map<String, LookupExtractorFactoryMapContainer>> EMPTY_TIERED_LOOKUP = ImmutableMap.of();
  private static final LookupsStateWithMap LOOKUPS_STATE = new LookupsStateWithMap(
      SINGLE_LOOKUP_MAP_V0,
      SINGLE_LOOKUP_MAP_V1,
      Collections.EMPTY_SET
  );

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
  public void testUpdateNodeWithSuccess() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(
                   StringUtils.toUtf8(
                       mapper.writeValueAsString(
                           LOOKUPS_STATE
                       )
                   )));
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
        returnCode.set(Response.Status.ACCEPTED.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    LookupsStateWithMap resp = manager.updateNode(
        HostAndPort.fromString("localhost"),
        LOOKUPS_STATE
    );

    EasyMock.verify(client, responseHandler);
    Assert.assertEquals(resp, LOOKUPS_STATE);
  }

  @Test
  public void testUpdateNodeRespondedWithNotOkErrorCode() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream("server failed".getBytes()));
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
        returnCode.set(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    try {
      manager.updateNode(
          HostAndPort.fromString("localhost"),
          LOOKUPS_STATE
      );
      Assert.fail();
    }
    catch (IOException ex) {
    }

    EasyMock.verify(client, responseHandler);
  }

  @Test
  public void testUpdateNodeReturnsWeird() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream("weird".getBytes()));
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
        returnCode.set(Response.Status.ACCEPTED.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    try {
      manager.updateNode(
          HostAndPort.fromString("localhost"),
          LOOKUPS_STATE
      );
      Assert.fail();
    }
    catch (IOException ex) {
    }

    EasyMock.verify(client, responseHandler);
  }

  @Test
  public void testUpdateNodeInterrupted() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
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
        returnCode.set(Response.Status.ACCEPTED.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    Thread.currentThread().interrupt();
    try {
      manager.updateNode(
          HostAndPort.fromString("localhost"),
          LOOKUPS_STATE
      );
      Assert.fail();
    }
    catch (InterruptedException ex) {
    }
    finally {
      //clear the interrupt
      Thread.interrupted();
    }

    EasyMock.verify(client, responseHandler);
  }


  @Test
  public void testGetLookupsStateNodeWithSuccess() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(
                   StringUtils.toUtf8(
                       mapper.writeValueAsString(
                           LOOKUPS_STATE
                       )
                   )));
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
        returnCode.set(Response.Status.OK.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    LookupsStateWithMap resp = manager.getLookupStateForNode(
        HostAndPort.fromString("localhost")
    );

    EasyMock.verify(client, responseHandler);
    Assert.assertEquals(resp, LOOKUPS_STATE);
  }


  @Test
  public void testGetLookupsStateNodeRespondedWithNotOkErrorCode() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream("server failed".getBytes()));
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
        returnCode.set(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    try {
      manager.getLookupStateForNode(
          HostAndPort.fromString("localhost")
      );
      Assert.fail();
    }
    catch (IOException ex) {
    }

    EasyMock.verify(client, responseHandler);
  }

  @Test
  public void testGetLookupsStateNodeReturnsWeird() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream("weird".getBytes()));
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
        returnCode.set(Response.Status.ACCEPTED.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    try {
      manager.getLookupStateForNode(
          HostAndPort.fromString("localhost")
      );
      Assert.fail();
    }
    catch (IOException ex) {
    }

    EasyMock.verify(client, responseHandler);
  }

  @Test
  public void testGetLookupsStateNodeInterrupted() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final SettableFuture<InputStream> future = SettableFuture.create();
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
        returnCode.set(Response.Status.ACCEPTED.getStatusCode());
        reasonString.set("");
        return responseHandler;
      }
    };

    Thread.currentThread().interrupt();
    try {
      manager.getLookupStateForNode(
          HostAndPort.fromString("localhost")
      );
      Assert.fail();
    }
    catch (InterruptedException ex) {
    }
    finally {
      //clear the interrupt
      Thread.interrupted();
    }

    EasyMock.verify(client, responseHandler);
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return EMPTY_TIERED_LOOKUP;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(TIERED_LOOKUP_MAP_V0),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    manager.updateLookup(LOOKUP_TIER, SINGLE_LOOKUP_NAME, SINGLE_LOOKUP_SPEC_V0, auditInfo);
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return null;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    expectedException.expect(ISE.class);
    manager.updateLookups(TIERED_LOOKUP_MAP_V0, auditInfo);
  }

  @Test
  public void testUpdateLookupUpdates() throws Exception
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return TIERED_LOOKUP_MAP_V0;
      }
    };

    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(TIERED_LOOKUP_MAP_V1),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    manager.updateLookups(TIERED_LOOKUP_MAP_V1, auditInfo);
    EasyMock.verify(configManager);
  }

  @Test(expected = IAE.class)
  public void testUpdateLookupFailsSameVersionUpdates() throws Exception
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return TIERED_LOOKUP_MAP_V0;
      }
    };

    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    manager.updateLookups(TIERED_LOOKUP_MAP_V0, auditInfo);
  }

  @Test
  public void testUpdateLookupsOnlyAddsToTier() throws Exception
  {
    final LookupExtractorFactoryMapContainer ignore = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("prop", "old")
    );
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
            LOOKUP_TIER + "1",
            ImmutableMap.of("foo", new LookupExtractorFactoryMapContainer("v0", ImmutableMap.<String, Object>of("prop", "old"))),
            LOOKUP_TIER + "2",
            ImmutableMap.of("ignore", ignore)
        );
      }
    };
    final LookupExtractorFactoryMapContainer newSpec = new LookupExtractorFactoryMapContainer(
        "v1",
        ImmutableMap.<String, Object>of("prop", "new")
    );
    EasyMock.reset(configManager);
    EasyMock.expect(
        configManager.set(
            EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
            EasyMock.eq(ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
                LOOKUP_TIER + "1", ImmutableMap.of("foo", newSpec),
                LOOKUP_TIER + "2", ImmutableMap.of("ignore", ignore)
            )),
            EasyMock.eq(auditInfo)
        )
    ).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.updateLookups(ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
        LOOKUP_TIER + "1", ImmutableMap.of(
            "foo",
            newSpec
        )
    ), auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testUpdateLookupsAddsNewTier() throws Exception
  {
    final LookupExtractorFactoryMapContainer ignore = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("prop", "old")
    );

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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
            LOOKUP_TIER + "2",
            ImmutableMap.of("ignore", ignore)
        );
      }
    };
    final LookupExtractorFactoryMapContainer newSpec = new LookupExtractorFactoryMapContainer(
        "v1",
        ImmutableMap.<String, Object>of("prop", "new")
    );
    EasyMock.reset(configManager);
    EasyMock.expect(
        configManager.set(
            EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
            EasyMock.eq(ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
                LOOKUP_TIER + "1", ImmutableMap.of("foo", newSpec),
                LOOKUP_TIER + "2", ImmutableMap.of("ignore", ignore)
            )),
            EasyMock.eq(auditInfo)
        )
    ).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.updateLookups(ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
        LOOKUP_TIER + "1", ImmutableMap.of(
            "foo",
            newSpec
        )
    ), auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testUpdateLookupsAddsNewLookup() throws Exception
  {
    final LookupExtractorFactoryMapContainer ignore = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("prop", "old")
    );

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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
            LOOKUP_TIER + "1",
            ImmutableMap.of(
                "foo1", new LookupExtractorFactoryMapContainer(
                    "v0", ImmutableMap.<String, Object>of("prop", "old")
                )
            ),
            LOOKUP_TIER + "2",
            ImmutableMap.of("ignore", ignore)
        );
      }
    };
    final LookupExtractorFactoryMapContainer newSpec = new LookupExtractorFactoryMapContainer(
        "v1",
        ImmutableMap.<String, Object>of("prop", "new")
    );
    EasyMock.reset(configManager);
    EasyMock.expect(
        configManager.set(
            EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
            EasyMock.eq(ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
                LOOKUP_TIER + "1", ImmutableMap.of(
                    "foo1", ignore,
                    "foo2", newSpec
                ),
                LOOKUP_TIER + "2", ImmutableMap.of("ignore", ignore)
            )),
            EasyMock.eq(auditInfo)
        )
    ).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.updateLookups(ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
        LOOKUP_TIER + "1", ImmutableMap.of(
            "foo2",
            newSpec
        )
    ), auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testDeleteLookup() throws Exception
  {
    final LookupExtractorFactoryMapContainer ignore = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("lookup", "ignore")
    );

    final LookupExtractorFactoryMapContainer lookup = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("lookup", "foo")
    );
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(LOOKUP_TIER, ImmutableMap.of(
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
    final LookupExtractorFactoryMapContainer ignore = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("lookup", "ignore")
    );
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(
            LOOKUP_TIER,
            ImmutableMap.of("ignore", ignore)
        );
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return null;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    Assert.assertFalse(manager.deleteLookup(LOOKUP_TIER, "foo", auditInfo));
  }

  @Test
  public void testGetLookup() throws Exception
  {
    final LookupExtractorFactoryMapContainer lookup = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("lookup", "foo")
    );
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(LOOKUP_TIER, ImmutableMap.of(
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
    final LookupExtractorFactoryMapContainer lookup = new LookupExtractorFactoryMapContainer(
        "v0",
        ImmutableMap.<String, Object>of("lookup", "foo")
    );
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager,
        lookupCoordinatorManagerConfig
    )
    {
      @Override
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(LOOKUP_TIER, ImmutableMap.of(
            "foo", lookup,
            "bar", new LookupExtractorFactoryMapContainer("v0",ImmutableMap.<String, Object>of())
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
      public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
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
