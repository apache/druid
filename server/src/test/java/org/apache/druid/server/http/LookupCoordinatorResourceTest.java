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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.google.common.net.HostAndPort;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LookupCoordinatorResourceTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final String LOOKUP_TIER = "lookupTier";
  private static final String LOOKUP_NAME = "lookupName";
  private static final LookupExtractorFactoryMapContainer SINGLE_LOOKUP = new LookupExtractorFactoryMapContainer(
      "v0",
      ImmutableMap.of()
  );
  private static final Map<String, LookupExtractorFactoryMapContainer> SINGLE_LOOKUP_MAP = ImmutableMap.of(
      LOOKUP_NAME,
      SINGLE_LOOKUP
  );
  private static final Map<String, Map<String, LookupExtractorFactoryMapContainer>> SINGLE_TIER_MAP = ImmutableMap.of(
      LOOKUP_TIER,
      SINGLE_LOOKUP_MAP
  );
  private static final ByteSource SINGLE_TIER_MAP_SOURCE = new ByteSource()
  {
    @Override
    public InputStream openStream() throws IOException
    {
      return new ByteArrayInputStream(StringUtils.toUtf8(MAPPER.writeValueAsString(SINGLE_TIER_MAP)));
    }
  };
  private static final ByteSource EMPTY_MAP_SOURCE = new ByteSource()
  {
    @Override
    public InputStream openStream() throws IOException
    {
      return new ByteArrayInputStream(StringUtils.toUtf8(MAPPER.writeValueAsString(SINGLE_LOOKUP)));
    }
  };

  private static final HostAndPort LOOKUP_NODE = HostAndPort.fromParts("localhost", 1111);

  private static final LookupsState<LookupExtractorFactoryMapContainer> LOOKUP_STATE = new LookupsState(
      ImmutableMap.of(LOOKUP_NAME, SINGLE_LOOKUP), null, null
  );

  private static final Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> NODES_LOOKUP_STATE =
      ImmutableMap.of(LOOKUP_NODE, LOOKUP_STATE);
  private static final AuthenticationResult AUTH_RESULT
      = new AuthenticationResult("id", "authorizer", "authBy", Collections.emptyMap());
  private final AuditInfo auditInfo
      = new AuditInfo("some author", "id", "some comment", "127.0.0.1");

  @Test
  public void testEnabledBulkReadFiltersLookups()
  {
    final LookupCoordinatorManager manager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.expect(manager.getKnownLookups()).andReturn(ImmutableMap.of(
        "prod", ImmutableMap.of("orders", SINGLE_LOOKUP, "users", SINGLE_LOOKUP),
        "dev", ImmutableMap.of("orders", SINGLE_LOOKUP),
        "empty", ImmutableMap.of()
    ));
    EasyMock.replay(manager);

    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(AUTH_RESULT);
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) ->
        "prod/orders".equals(resource.getName()) || "empty".equals(resource.getName())
        ? Access.OK : Access.DENIED);
    final LookupCoordinatorResource resource = new LookupCoordinatorResource(
        manager,
        MAPPER,
        MAPPER,
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build()
    );

    final Response response = resource.getAllLookupSpecs(request);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        ImmutableMap.of("prod", ImmutableMap.of("orders", SINGLE_LOOKUP), "empty", ImmutableMap.of()),
        response.getEntity()
    );
    EasyMock.verify(manager);
  }

  @Test
  public void testEnabledBulkWriteRequiresEveryLookup()
  {
    final LookupCoordinatorManager manager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.replay(manager);
    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(AUTH_RESULT);
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) ->
        "prod/orders".equals(resource.getName()) ? Access.OK : Access.DENIED);
    final LookupCoordinatorResource resource = new LookupCoordinatorResource(
        manager,
        MAPPER,
        MAPPER,
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build()
    );
    final InputStream body = new ByteArrayInputStream(StringUtils.toUtf8(
        "{\"prod\":{\"orders\":{\"version\":\"v0\",\"lookupExtractorFactory\":{}},"
        + "\"users\":{\"version\":\"v0\",\"lookupExtractorFactory\":{}}}}"
    ));

    final Response response = resource.updateAllLookups(body, request);
    Assertions.assertEquals(403, response.getStatus());
    Assertions.assertTrue(((Map<?, ?>) response.getEntity()).containsKey("Access-Check-Result"));
    EasyMock.verify(manager);
  }

  @Test
  public void testEnabledNodeStatusHidesUnreadableHost()
  {
    final HostAndPort deniedHost = HostAndPort.fromParts("denied", 1111);
    final LookupCoordinatorManager manager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.expect(manager.getLastKnownLookupsStateOnNodes()).andReturn(ImmutableMap.of(
        LOOKUP_NODE, LOOKUP_STATE,
        deniedHost, new LookupsState<>(ImmutableMap.of("other", SINGLE_LOOKUP), null, null)
    ));
    EasyMock.expect(manager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE, deniedHost));
    EasyMock.replay(manager);

    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(AUTH_RESULT);
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) ->
        (LOOKUP_TIER + "/" + LOOKUP_NAME).equals(resource.getName()) ? Access.OK : Access.DENIED);
    final LookupCoordinatorResource resource = new LookupCoordinatorResource(
        manager,
        MAPPER,
        MAPPER,
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build()
    );

    final Response response = resource.getNodesStatusInTier(LOOKUP_TIER, request);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of(LOOKUP_NODE, LOOKUP_STATE), response.getEntity());
    EasyMock.verify(manager);
  }

  @Test
  public void testEnabledDeleteTierReturnsForbiddenForDeniedLookup()
  {
    final LookupCoordinatorManager manager = mock(LookupCoordinatorManager.class);
    doAnswer(call -> {
      final Consumer<Map<String, LookupExtractorFactoryMapContainer>> checkLookups = call.getArgument(2);
      checkLookups.accept(SINGLE_LOOKUP_MAP);
      return true;
    }).when(manager).deleteTier(eq(LOOKUP_TIER), any(), any());

    final AuthorizerMapper mapper = mock(AuthorizerMapper.class);
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(AUTH_RESULT);
    when(mapper.getAuthorizer("authorizer")).thenReturn((authentication, resource, action) -> Access.DENIED);
    final LookupCoordinatorResource resource = new LookupCoordinatorResource(
        manager,
        MAPPER,
        MAPPER,
        mapper,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build()
    );

    final Response response = resource.deleteTier(LOOKUP_TIER, request);
    Assertions.assertEquals(403, response.getStatus());
    Assertions.assertTrue(((Map<?, ?>) response.getEntity()).containsKey("Access-Check-Result"));
  }

  @Test
  public void testSimpleGet()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    final Map<String, Map<String, LookupExtractorFactoryMapContainer>> retVal = new HashMap<>();
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(retVal).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getTiers(false, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(retVal.keySet(), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testMissingGet()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(null).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getTiers(false, null);
    Assertions.assertEquals(404, response.getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testExceptionalGet()
  {
    final String errMsg = "some error";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andThrow(new RuntimeException(errMsg)).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getTiers(false, null);
    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testDiscoveryGet()
  {
    final Set<String> tiers = ImmutableSet.of("discoveredLookupTier");
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP).once();
    EasyMock.expect(lookupCoordinatorManager.discoverTiers()).andReturn(tiers).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getTiers(true, null);
    Assertions.assertEquals(200, response.getStatus());

    Assertions.assertEquals(ImmutableSet.of("lookupTier", "discoveredLookupTier"), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testDiscoveryExceptionalGet()
  {
    final String errMsg = "some error";
    final RuntimeException ex = new RuntimeException(errMsg);
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP).once();
    EasyMock.expect(lookupCoordinatorManager.discoverTiers()).andThrow(ex).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getTiers(true, null);
    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleGetLookup()
  {
    final LookupExtractorFactoryMapContainer container = new LookupExtractorFactoryMapContainer(
        "v0",
        new HashMap<>()
    );
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(LOOKUP_TIER), EasyMock.eq(LOOKUP_NAME)))
            .andReturn(container)
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificLookup(LOOKUP_TIER, LOOKUP_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(container, response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testDetailedGetLookup()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(LOOKUP_TIER, true, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(SINGLE_TIER_MAP.get(LOOKUP_TIER), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testMissingGetLookup()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(LOOKUP_TIER), EasyMock.eq(LOOKUP_NAME)))
            .andReturn(null)
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificLookup(LOOKUP_TIER, LOOKUP_NAME);
    Assertions.assertEquals(404, response.getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testInvalidGetLookup()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    Assertions.assertEquals(400, lookupCoordinatorResource.getSpecificLookup("foo", null).getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.getSpecificLookup("foo", "").getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.getSpecificLookup("", "foo").getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.getSpecificLookup(null, "foo").getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testExceptionalGetLookup()
  {
    final String errMsg = "some message";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(LOOKUP_TIER), EasyMock.eq(LOOKUP_NAME)))
            .andThrow(new RuntimeException(errMsg))
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificLookup(LOOKUP_TIER, LOOKUP_NAME);
    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleDeleteTier()
  {
    final HttpServletRequest request = createMockRequest(false);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteTier(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.capture(auditInfoCapture),
        EasyMock.anyObject()
    )).andReturn(true).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.deleteTier(
        LOOKUP_TIER,
        request
    );

    Assertions.assertEquals(202, response.getStatus());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testSimpleDelete()
  {
    final HttpServletRequest request = createMockRequest(false);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(true).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.deleteLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        request
    );

    Assertions.assertEquals(202, response.getStatus());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testMissingDelete()
  {
    final HttpServletRequest request = createMockRequest(false);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(false).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.deleteLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        request
    );

    Assertions.assertEquals(404, response.getStatus());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testExceptionalDelete()
  {
    final String errMsg = "some error";

    final HttpServletRequest request = createMockRequest(false);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.deleteLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.capture(auditInfoCapture)
    )).andThrow(new RuntimeException(errMsg)).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.deleteLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        request
    );

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testInvalidDelete()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    Assertions.assertEquals(400, lookupCoordinatorResource.deleteLookup("foo", null, null).getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.deleteLookup(null, null, null).getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.deleteLookup(null, "foo", null).getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.deleteLookup("foo", "", null).getStatus());
    Assertions.assertEquals(400, lookupCoordinatorResource.deleteLookup("", "foo", null).getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleNew() throws Exception
  {
    final HttpServletRequest request = createMockRequest(true);
    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookups(
        EasyMock.eq(SINGLE_TIER_MAP),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(true).once();

    EasyMock.replay(lookupCoordinatorManager, request);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.updateAllLookups(
        SINGLE_TIER_MAP_SOURCE.openStream(),
        request
    );

    Assertions.assertEquals(202, response.getStatus());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testExceptionalNew() throws Exception
  {
    final String errMsg = "some error";

    final HttpServletRequest request = createMockRequest(true);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookups(
        EasyMock.eq(SINGLE_TIER_MAP),
        EasyMock.capture(auditInfoCapture)
    )).andThrow(new RuntimeException(errMsg)).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.updateAllLookups(
        SINGLE_TIER_MAP_SOURCE.openStream(),
        request
    );

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testFailedNew() throws Exception
  {
    final HttpServletRequest request = createMockRequest(true);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookups(
        EasyMock.eq(SINGLE_TIER_MAP),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(false).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.updateAllLookups(
        SINGLE_TIER_MAP_SOURCE.openStream(),
        request
    );

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "Unknown error updating configuration"), response.getEntity());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testSimpleNewLookup() throws Exception
  {
    final HttpServletRequest request = createMockRequest(true);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.eq(SINGLE_LOOKUP),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(true).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        EMPTY_MAP_SOURCE.openStream(),
        request
    );

    Assertions.assertEquals(202, response.getStatus());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testDBErrNewLookup() throws Exception
  {
    final HttpServletRequest request = createMockRequest(true);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.eq(SINGLE_LOOKUP),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(false).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        EMPTY_MAP_SOURCE.openStream(),
        request
    );

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "Unknown error updating configuration"), response.getEntity());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testExceptionalNewLookup() throws Exception
  {
    final String errMsg = "error message";

    final HttpServletRequest request = createMockRequest(true);

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.eq(SINGLE_LOOKUP),
        EasyMock.capture(auditInfoCapture)
    )).andThrow(new RuntimeException(errMsg)).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        EMPTY_MAP_SOURCE.openStream(),
        request
    );

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assertions.assertTrue(auditInfoCapture.hasCaptured());
    Assertions.assertEquals(auditInfo, auditInfoCapture.getValue());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testNullValsNewLookup() throws Exception
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);

    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    EasyMock.replay(lookupCoordinatorManager, request);
    Assertions.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        null,
        LOOKUP_NAME,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());

    Assertions.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        null,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());

    Assertions.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        "",
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());

    Assertions.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        "",
        LOOKUP_NAME,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());
    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testSimpleGetTier()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(LOOKUP_TIER, false, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(SINGLE_TIER_MAP.get(LOOKUP_TIER).keySet(), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testMissingGetTier()
  {
    final String tier = "some tier";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    final Map<String, Map<String, Map<String, Object>>> retVal =
        ImmutableMap.of();
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier, false, null);
    Assertions.assertEquals(404, response.getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testNullGetTier()
  {
    final String tier = null;
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier, false, null);
    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "`tier` required"), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testNullLookupsGetTier()
  {
    final String tier = "some tier";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(null).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier, false, null);
    Assertions.assertEquals(404, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "No lookups found"), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testExceptionalGetTier()
  {
    final String tier = "some tier";
    final String errMsg = "some error";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andThrow(new RuntimeException(errMsg)).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier, false, null);
    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetAllLookupsStatus()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP);
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE));

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getAllLookupsStatus(false, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        ImmutableMap.of(
            LOOKUP_TIER,
            ImmutableMap.of(
                LOOKUP_NAME,
                new LookupCoordinatorResource.LookupStatus(true, null)
            )
        ), response.getEntity()
    );

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetLookupStatusForTier()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP);
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE));
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getLookupStatusForTier(LOOKUP_TIER, false, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        ImmutableMap.of(
            LOOKUP_NAME,
            new LookupCoordinatorResource.LookupStatus(true, null)
        ), response.getEntity()
    );

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetSpecificLookupStatus()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP);
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE));
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getSpecificLookupStatus(LOOKUP_TIER, LOOKUP_NAME, false);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        new LookupCoordinatorResource.LookupStatus(true, null), response.getEntity()
    );

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetLookupStatusDetailedTrue()
  {
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        EasyMock.createStrictMock(LookupCoordinatorManager.class),
        MAPPER,
        MAPPER
    );

    HostAndPort newNode = HostAndPort.fromParts("localhost", 4352);
    Assertions.assertEquals(
        new LookupCoordinatorResource.LookupStatus(false, ImmutableList.of(newNode)),
        lookupCoordinatorResource.getLookupStatus(
            LOOKUP_NAME,
            SINGLE_LOOKUP,
            ImmutableList.of(LOOKUP_NODE, newNode),
            NODES_LOOKUP_STATE,
            true
        )
    );
  }

  @Test
  public void testGetLookupStatusDetailedFalse()
  {
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        EasyMock.createStrictMock(LookupCoordinatorManager.class),
        MAPPER,
        MAPPER
    );

    HostAndPort newNode = HostAndPort.fromParts("localhost", 4352);
    Assertions.assertEquals(
        new LookupCoordinatorResource.LookupStatus(false, null),
        lookupCoordinatorResource.getLookupStatus(
            LOOKUP_NAME,
            SINGLE_LOOKUP,
            ImmutableList.of(LOOKUP_NODE, newNode),
            NODES_LOOKUP_STATE,
            false
        )
    );
  }

  @Test
  public void testGetAllNodesStatus()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP);
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE));

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getAllNodesStatus(false, null, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        ImmutableMap.of(
            LOOKUP_TIER,
            ImmutableMap.of(
                LOOKUP_NODE,
                LOOKUP_STATE
            )
        ),
        response.getEntity()
    );

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetAllNodesStatusDetailedFalse()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(SINGLE_TIER_MAP);
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE));

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getAllNodesStatus(false, false, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        ImmutableMap.of(
            LOOKUP_TIER,
            ImmutableMap.of(
                LOOKUP_NODE,
                new LookupsState(
                    ImmutableMap.of(LOOKUP_NAME, SINGLE_LOOKUP.getVersion()), null, null
                )
            )
        ),
        response.getEntity()
    );

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetNodesStatusInTier()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of(LOOKUP_NODE));

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getNodesStatusInTier(LOOKUP_TIER, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(
        ImmutableMap.of(
            LOOKUP_NODE,
            LOOKUP_STATE
        ), response.getEntity()
    );

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetSpecificNodeStatus()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getLastKnownLookupsStateOnNodes()).andReturn(NODES_LOOKUP_STATE);

    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );

    final Response response = lookupCoordinatorResource.getSpecificNodeStatus(LOOKUP_TIER, LOOKUP_NODE, null);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(LOOKUP_STATE, response.getEntity());

    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetSpecificNodeStatusRejectsHostInAnotherTier()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.discoverNodesInTier(LOOKUP_TIER)).andReturn(ImmutableList.of());
    EasyMock.replay(lookupCoordinatorManager);

    final LookupCoordinatorResource resource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER,
        null,
        AuthConfig.newBuilder().setEnableLookupAuthorization(true).build()
    );
    final HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.replay(request);

    Assertions.assertEquals(404, resource.getSpecificNodeStatus(LOOKUP_TIER, LOOKUP_NODE, request).getStatus());
    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testGetAllLookupSpecs()
  {
    final Map<String, Map<String, LookupExtractorFactoryMapContainer>> lookups = ImmutableMap.of(
        "tier1",
        ImmutableMap.of(
            "lookup1",
            new LookupExtractorFactoryMapContainer(
                "v0",
                ImmutableMap.of("k1", "v2")
            ),
            "lookup2",
            new LookupExtractorFactoryMapContainer(
                "v1",
                ImmutableMap.of("k", "v")
            )
        ),
        "tier2",
        ImmutableMap.of(
            "lookup1",
            new LookupExtractorFactoryMapContainer(
                "v0",
                ImmutableMap.of("k1", "v2")
            )
        )
    );
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups())
            .andReturn(lookups)
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getAllLookupSpecs(null);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(lookups, response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testGetEmptyAllLookupSpecs()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class
    );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups())
            .andReturn(null)
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        MAPPER,
        MAPPER
    );
    final Response response = lookupCoordinatorResource.getAllLookupSpecs(null);
    Assertions.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  private HttpServletRequest createMockRequest(boolean hasJsonPayload)
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    if (hasJsonPayload) {
      EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    }
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("some author").once();
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("some comment").once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(AUTH_RESULT).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1").once();

    return request;
  }
}
