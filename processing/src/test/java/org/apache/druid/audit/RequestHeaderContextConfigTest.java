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

package org.apache.druid.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class RequestHeaderContextConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testDefaultIncludesTraceId()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig();
    Assertions.assertEquals(
        "traceId",
        config.getHeaderToContextKey().get(AuditManager.X_DRUID_TRACE_ID)
    );
  }

  @Test
  public void testDeserFromJson() throws IOException
  {
    String json = "{\"headerToContextKey\":{\"X-Custom-Foo\":\"foo\",\"X-Druid-Trace-Id\":\"traceId\"}}";
    RequestHeaderContextConfig parsed = mapper.readValue(json, RequestHeaderContextConfig.class);
    Assertions.assertEquals("foo", parsed.getHeaderToContextKey().get("X-Custom-Foo"));
    Assertions.assertEquals("traceId", parsed.getHeaderToContextKey().get("X-Druid-Trace-Id"));
  }

  @Test
  public void testEmptyMapDisablesFeature() throws IOException
  {
    String json = "{\"headerToContextKey\":{}}";
    RequestHeaderContextConfig parsed = mapper.readValue(json, RequestHeaderContextConfig.class);
    Assertions.assertTrue(parsed.getHeaderToContextKey().isEmpty());
  }

  @Test
  public void testEmptyJsonObjectFallsBackToDefault() throws IOException
  {
    // Critical: JsonConfigProvider does convertValue({}, RequestHeaderContextConfig.class)
    // when nothing is configured under druid.audit.requestHeaders.*; Jackson invokes the
    // @JsonCreator with headerToContextKey=null. That must NOT silently disable the feature.
    RequestHeaderContextConfig parsed = mapper.readValue("{}", RequestHeaderContextConfig.class);
    Assertions.assertEquals(
        "traceId",
        parsed.getHeaderToContextKey().get(AuditManager.X_DRUID_TRACE_ID)
    );
  }

  @Test
  public void testMappingToReservedQueryIdRejected()
  {
    final String json = "{\"headerToContextKey\":{\"X-My-Header\":\"queryId\"}}";
    Assertions.assertThrows(
        com.fasterxml.jackson.databind.JsonMappingException.class,
        () -> mapper.readValue(json, RequestHeaderContextConfig.class)
    );
  }

  @Test
  public void testMappingToReservedSqlQueryIdRejected()
  {
    final String json = "{\"headerToContextKey\":{\"X-Foo\":\"sqlQueryId\"}}";
    Assertions.assertThrows(
        com.fasterxml.jackson.databind.JsonMappingException.class,
        () -> mapper.readValue(json, RequestHeaderContextConfig.class)
    );
  }

  @Test
  public void testGetHeaderToContextKeyIsImmutable()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> config.getHeaderToContextKey().put("X-Evil", "evil")
    );
  }

  @Test
  public void testApplyToOutboundRequest_attachesPresentHeadersFromContext()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig();
    java.util.Map<String, Object> ctx = com.google.common.collect.ImmutableMap.of(
        "traceId", "abc-123",
        "otherKey", "ignored"
    );
    java.util.Map<String, String> captured = new java.util.HashMap<>();
    config.applyToOutboundRequest(ctx, captured::put);
    Assertions.assertEquals(1, captured.size());
    Assertions.assertEquals("abc-123", captured.get(AuditManager.X_DRUID_TRACE_ID));
  }

  @Test
  public void testApplyToOutboundRequest_skipsAbsentKeys()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig();
    java.util.Map<String, String> captured = new java.util.HashMap<>();
    config.applyToOutboundRequest(java.util.Collections.emptyMap(), captured::put);
    Assertions.assertTrue(captured.isEmpty());
  }

  @Test
  public void testApplyToOutboundRequest_nullContextIsNoOp()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig();
    java.util.Map<String, String> captured = new java.util.HashMap<>();
    config.applyToOutboundRequest(null, captured::put);
    Assertions.assertTrue(captured.isEmpty());
  }

  @Test
  public void testApplyCapturedHeaders_attachesByHeaderName()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig(
        com.google.common.collect.ImmutableMap.of(
            AuditManager.X_DRUID_TRACE_ID, "traceId",
            "X-Tenant-Id", "tenantId"
        )
    );
    // keyed by context key, as held by RequestHeaderContext
    java.util.Map<String, String> capturedByContextKey = com.google.common.collect.ImmutableMap.of(
        "traceId", "abc-123",
        "tenantId", "acme"
    );
    java.util.Map<String, String> outbound = new java.util.HashMap<>();
    config.applyCapturedHeaders(capturedByContextKey, outbound::put);
    Assertions.assertEquals("abc-123", outbound.get(AuditManager.X_DRUID_TRACE_ID));
    Assertions.assertEquals("acme", outbound.get("X-Tenant-Id"));
  }

  @Test
  public void testApplyCapturedHeaders_emptyOrNullIsNoOp()
  {
    RequestHeaderContextConfig config = new RequestHeaderContextConfig();
    java.util.Map<String, String> outbound = new java.util.HashMap<>();
    config.applyCapturedHeaders(java.util.Collections.emptyMap(), outbound::put);
    config.applyCapturedHeaders(null, outbound::put);
    Assertions.assertTrue(outbound.isEmpty());
  }
}
