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

package org.apache.druid.server.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.audit.RequestHeaderContextConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class RequestHeaderContextFilterTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @After
  public void clearTraceId()
  {
    RequestHeaderContext.clear();
  }

  @Test
  public void testHeaderCapturedBoundForChainAndClearedAfter() throws Exception
  {
    RequestHeaderContextFilter filter = new RequestHeaderContextFilter(new RequestHeaderContextConfig());

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(AuditManager.X_DRUID_TRACE_ID)).thenReturn("trace-xyz");

    AtomicReference<Map<String, String>> observed = new AtomicReference<>();
    FilterChain chain = (req, resp) -> observed.set(RequestHeaderContext.current());

    filter.doFilter(request, response, chain);

    Assert.assertEquals("trace-xyz", observed.get().get("traceId"));
    Assert.assertTrue(
        "thread-local must be cleared after the filter chain",
        RequestHeaderContext.current().isEmpty()
    );
  }

  @Test
  public void testMultipleConfiguredHeaders() throws Exception
  {
    String json = "{\"headerToContextKey\":{\"X-Druid-Trace-Id\":\"traceId\",\"X-Custom-Foo\":\"foo\"}}";
    RequestHeaderContextConfig config = mapper.readValue(json, RequestHeaderContextConfig.class);
    RequestHeaderContextFilter filter = new RequestHeaderContextFilter(config);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(AuditManager.X_DRUID_TRACE_ID)).thenReturn("trace-1");
    Mockito.when(request.getHeader("X-Custom-Foo")).thenReturn("foo-1");

    AtomicReference<Map<String, String>> observed = new AtomicReference<>();
    FilterChain chain = (req, resp) -> observed.set(RequestHeaderContext.current());

    filter.doFilter(request, response, chain);

    Assert.assertEquals("trace-1", observed.get().get("traceId"));
    Assert.assertEquals("foo-1", observed.get().get("foo"));
  }

  @Test
  public void testNoHeadersPresentIsNoOp() throws Exception
  {
    RequestHeaderContextFilter filter = new RequestHeaderContextFilter(new RequestHeaderContextConfig());

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(AuditManager.X_DRUID_TRACE_ID)).thenReturn(null);

    AtomicReference<Map<String, String>> observed = new AtomicReference<>();
    FilterChain chain = (req, resp) -> observed.set(RequestHeaderContext.current());

    filter.doFilter(request, response, chain);

    Assert.assertTrue(observed.get().isEmpty());
    Assert.assertTrue(RequestHeaderContext.current().isEmpty());
  }

  @Test
  public void testEmptyHeaderValueTreatedAsAbsent() throws Exception
  {
    RequestHeaderContextFilter filter = new RequestHeaderContextFilter(new RequestHeaderContextConfig());

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(AuditManager.X_DRUID_TRACE_ID)).thenReturn("");

    AtomicReference<Map<String, String>> observed = new AtomicReference<>();
    FilterChain chain = (req, resp) -> observed.set(RequestHeaderContext.current());

    filter.doFilter(request, response, chain);

    Assert.assertTrue(observed.get().isEmpty());
  }

  @Test
  public void testClearedEvenIfChainThrows() throws IOException
  {
    RequestHeaderContextFilter filter = new RequestHeaderContextFilter(new RequestHeaderContextConfig());

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(AuditManager.X_DRUID_TRACE_ID)).thenReturn("trace-throws");

    FilterChain chain = (req, resp) -> {
      throw new RuntimeException("boom");
    };

    Assert.assertThrows(RuntimeException.class, () -> filter.doFilter(request, response, chain));
    Assert.assertTrue(
        "thread-local must be cleared even on exception",
        RequestHeaderContext.current().isEmpty()
    );
  }

  @Test
  public void testEmptyConfigIsNoOp() throws Exception
  {
    RequestHeaderContextConfig empty = mapper.readValue("{\"headerToContextKey\":{}}", RequestHeaderContextConfig.class);
    RequestHeaderContextFilter filter = new RequestHeaderContextFilter(empty);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    // Even if a header is present, an empty config shouldn't capture anything.
    Mockito.when(request.getHeader(AuditManager.X_DRUID_TRACE_ID)).thenReturn("trace-1");

    AtomicReference<Map<String, String>> observed = new AtomicReference<>();
    FilterChain chain = (req, resp) -> observed.set(RequestHeaderContext.current());

    filter.doFilter(request, response, chain);

    Assert.assertTrue(observed.get().isEmpty());
  }
}
