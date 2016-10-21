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

package io.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import io.druid.audit.AuditInfo;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.server.lookup.cache.LookupCoordinatorManager;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LookupCoordinatorResourceTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();
  private static final String LOOKUP_TIER = "lookupTier";
  private static final String LOOKUP_NAME = "lookupName";
  private static final Map<String, Map<String, Object>> SINGLE_LOOKUP_MAP = ImmutableMap.<String, Map<String, Object>>of(
      LOOKUP_NAME,
      ImmutableMap.<String, Object>of()
  );
  private static final Map<String, Map<String, Map<String, Object>>> SINGLE_TIER_MAP = ImmutableMap.<String, Map<String, Map<String, Object>>>of(
      LOOKUP_TIER,
      SINGLE_LOOKUP_MAP
  );
  private static final ByteSource SINGLE_TIER_MAP_SOURCE = new ByteSource()
  {
    @Override
    public InputStream openStream() throws IOException
    {
      return new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(SINGLE_TIER_MAP)));
    }
  };
  private static final ByteSource EMPTY_MAP_SOURCE = new ByteSource()
  {
    @Override
    public InputStream openStream() throws IOException
    {
      return new ByteArrayInputStream(StringUtils.toUtf8(mapper.writeValueAsString(ImmutableMap.of())));
    }
  };

  @Test
  public void testSimpleGet()
  {
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    final Map<String, Map<String, Map<String, Object>>> retVal = new HashMap<>();
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(retVal).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getTiers(false);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(retVal.keySet(), response.getEntity());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getTiers(false);
    Assert.assertEquals(404, response.getStatus());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getTiers(false);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testDiscoveryGet()
  {
    final List<String> tiers = ImmutableList.of();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.discoverTiers()).andReturn(tiers).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getTiers(true);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(tiers, response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testDiscoveryExceptionalGet()
  {
    final List<String> tiers = ImmutableList.of();
    final String errMsg = "some error";
    final RuntimeException ex = new RuntimeException(errMsg);
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.discoverTiers()).andThrow(ex).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getTiers(true);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleGetLookup()
  {
    final Map<String, Object> map = new HashMap<>();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.getLookup(EasyMock.eq(LOOKUP_TIER), EasyMock.eq(LOOKUP_NAME)))
            .andReturn(map)
            .once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificLookup(LOOKUP_TIER, LOOKUP_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(map, response.getEntity());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificLookup(LOOKUP_TIER, LOOKUP_NAME);
    Assert.assertEquals(404, response.getStatus());
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
        mapper,
        mapper
    );
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificLookup("foo", null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificLookup("foo", "").getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificLookup("", "foo").getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.getSpecificLookup(null, "foo").getStatus());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificLookup(LOOKUP_TIER, LOOKUP_NAME);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleDelete()
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.deleteLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        author,
        comment,
        request
    );

    Assert.assertEquals(202, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testMissingDelete()
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.deleteLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        author,
        comment,
        request
    );

    Assert.assertEquals(404, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testExceptionalDelete()
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";
    final String errMsg = "some error";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.deleteLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        author,
        comment,
        request
    );

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

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
        mapper,
        mapper
    );
    Assert.assertEquals(400, lookupCoordinatorResource.deleteLookup("foo", null, null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteLookup(null, null, null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteLookup(null, "foo", null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteLookup("foo", "", null, null, null).getStatus());
    Assert.assertEquals(400, lookupCoordinatorResource.deleteLookup("", "foo", null, null, null).getStatus());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testSimpleNew() throws Exception
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.updateAllLookups(
        SINGLE_TIER_MAP_SOURCE.openStream(),
        author,
        comment,
        request
    );

    Assert.assertEquals(202, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testExceptionalNew() throws Exception
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";
    final String errMsg = "some error";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.updateAllLookups(
        SINGLE_TIER_MAP_SOURCE.openStream(),
        author,
        comment,
        request
    );

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testFailedNew() throws Exception
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.updateAllLookups(
        SINGLE_TIER_MAP_SOURCE.openStream(),
        author,
        comment,
        request
    );

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "Unknown error updating configuration"), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testSimpleNewLookup() throws Exception
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.eq(ImmutableMap.<String, Object>of()),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(true).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    );

    Assert.assertEquals(202, response.getStatus());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testDBErrNewLookup() throws Exception
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.eq(ImmutableMap.<String, Object>of()),
        EasyMock.capture(auditInfoCapture)
    )).andReturn(false).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    );

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "Unknown error updating configuration"), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testExceptionalNewLookup() throws Exception
  {
    final String errMsg = "error message";
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getContentType()).andReturn(MediaType.APPLICATION_JSON).once();
    EasyMock.expect(request.getRemoteAddr()).andReturn(ip).once();

    final Capture<AuditInfo> auditInfoCapture = Capture.newInstance();
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);
    EasyMock.expect(lookupCoordinatorManager.updateLookup(
        EasyMock.eq(LOOKUP_TIER),
        EasyMock.eq(LOOKUP_NAME),
        EasyMock.eq(ImmutableMap.<String, Object>of()),
        EasyMock.capture(auditInfoCapture)
    )).andThrow(new RuntimeException(errMsg)).once();

    EasyMock.replay(lookupCoordinatorManager, request);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        LOOKUP_NAME,
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    );

    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    Assert.assertTrue(auditInfoCapture.hasCaptured());
    final AuditInfo auditInfo = auditInfoCapture.getValue();
    Assert.assertEquals(author, auditInfo.getAuthor());
    Assert.assertEquals(comment, auditInfo.getComment());
    Assert.assertEquals(ip, auditInfo.getIp());

    EasyMock.verify(lookupCoordinatorManager, request);
  }


  @Test
  public void testNullValsNewLookup() throws Exception
  {
    final String author = "some author";
    final String comment = "some comment";
    final String ip = "127.0.0.1";

    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);

    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(
        LookupCoordinatorManager.class);

    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );

    EasyMock.replay(lookupCoordinatorManager, request);
    Assert.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        null,
        LOOKUP_NAME,
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());

    Assert.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        null,
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());

    Assert.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        LOOKUP_TIER,
        "",
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());

    Assert.assertEquals(400, lookupCoordinatorResource.createOrUpdateLookup(
        "",
        LOOKUP_NAME,
        author,
        comment,
        EMPTY_MAP_SOURCE.openStream(),
        request
    ).getStatus());
    EasyMock.verify(lookupCoordinatorManager, request);
  }

  @Test
  public void testSimpleGetTier()
  {
    final String tier = "some tier";
    final String lookup = "some lookup";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    final Map<String, Map<String, Map<String, Object>>> retVal =
        ImmutableMap.<String, Map<String, Map<String, Object>>>of(
            tier, ImmutableMap.<String, Map<String, Object>>of(lookup, ImmutableMap.<String, Object>of())
        );
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(retVal).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(retVal.get(tier).keySet(), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }

  @Test
  public void testMissingGetTier()
  {
    final String tier = "some tier";
    final LookupCoordinatorManager lookupCoordinatorManager = EasyMock.createStrictMock(LookupCoordinatorManager.class);
    final Map<String, Map<String, Map<String, Object>>> retVal =
        ImmutableMap.<String, Map<String, Map<String, Object>>>of();
    EasyMock.expect(lookupCoordinatorManager.getKnownLookups()).andReturn(retVal).once();
    EasyMock.replay(lookupCoordinatorManager);
    final LookupCoordinatorResource lookupCoordinatorResource = new LookupCoordinatorResource(
        lookupCoordinatorManager,
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier);
    Assert.assertEquals(404, response.getStatus());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "`tier` required"), response.getEntity());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier);
    Assert.assertEquals(404, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "No lookups found"), response.getEntity());
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
        mapper,
        mapper
    );
    final Response response = lookupCoordinatorResource.getSpecificTier(tier);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", errMsg), response.getEntity());
    EasyMock.verify(lookupCoordinatorManager);
  }
}
