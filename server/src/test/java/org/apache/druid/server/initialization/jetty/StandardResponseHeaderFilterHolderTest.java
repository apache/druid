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

package org.apache.druid.server.initialization.jetty;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.initialization.ServerConfig;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StandardResponseHeaderFilterHolderTest
{
  public ServerConfig serverConfig;
  public HttpServletRequest httpRequest;
  public HttpServletResponse httpResponse;
  public FilterChain filterChain;

  @Before
  public void setUp()
  {
    serverConfig = EasyMock.strictMock(ServerConfig.class);
    httpRequest = EasyMock.strictMock(HttpServletRequest.class);
    httpResponse = EasyMock.strictMock(HttpServletResponse.class);
    filterChain = EasyMock.strictMock(FilterChain.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(serverConfig, httpRequest, httpResponse, filterChain);
  }

  @Test
  public void test_get_nullContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.GET).anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.<String, String>builder()
                    .put("Cache-Control", "no-cache, no-store, max-age=0")
                    .put("Content-Security-Policy", "frame-ancestors 'none'")
                    .build()
    );
  }

  @Test
  public void test_post_nullContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.POST).anyTimes();

    runFilterAndVerifyHeaders(Collections.emptyMap());
  }

  @Test
  public void test_get_emptyContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.GET).anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.<String, String>builder()
                    .put("Cache-Control", "no-cache, no-store, max-age=0")
                    .put("Content-Security-Policy", "frame-ancestors 'none'")
                    .build()
    );
  }

  @Test
  public void test_get_overrideContentSecurityPolicy() throws Exception
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("frame-ancestors 'self'").once();
    EasyMock.expect(httpRequest.getMethod()).andReturn(HttpMethod.GET).anyTimes();
    EasyMock.expect(httpResponse.getContentType()).andReturn("text/html").anyTimes();

    runFilterAndVerifyHeaders(
        ImmutableMap.<String, String>builder()
                    .put("Cache-Control", "no-cache, no-store, max-age=0")
                    .put("Content-Security-Policy", "frame-ancestors 'self'")
                    .build()
    );
  }

  @Test
  public void test_get_invalidContentSecurityPolicy()
  {
    EasyMock.expect(serverConfig.getContentSecurityPolicy()).andReturn("erroné").once();

    replayAllMocks();

    final RuntimeException e = Assert.assertThrows(RuntimeException.class, this::makeFilter);

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("Content-Security-Policy header value must be fully ASCII")
        )
    );
  }

  private StandardResponseHeaderFilterHolder.StandardResponseHeaderFilter makeFilter()
  {
    return (StandardResponseHeaderFilterHolder.StandardResponseHeaderFilter)
        new StandardResponseHeaderFilterHolder(serverConfig).getFilter();
  }

  private void runFilterAndVerifyHeaders(final Map<String, String> expectedHeaders) throws Exception
  {
    final Map<String, Capture<String>> captureMap = new HashMap<>();

    for (final Map.Entry<String, String> entry : expectedHeaders.entrySet()) {
      final String headerName = entry.getKey();
      final Capture<String> headerValueCapture = Capture.newInstance();
      captureMap.put(headerName, headerValueCapture);

      httpResponse.setHeader(EasyMock.eq(headerName), EasyMock.capture(headerValueCapture));
      EasyMock.expectLastCall();
    }

    filterChain.doFilter(httpRequest, httpResponse);
    EasyMock.expectLastCall();

    replayAllMocks();
    final StandardResponseHeaderFilterHolder.StandardResponseHeaderFilter filter = makeFilter();
    filter.doFilter(httpRequest, httpResponse, filterChain);

    for (final Map.Entry<String, String> entry : expectedHeaders.entrySet()) {
      Assert.assertEquals(entry.getKey(), entry.getValue(), captureMap.get(entry.getKey()).getValue());
    }
  }

  private void replayAllMocks()
  {
    EasyMock.replay(serverConfig, httpRequest, httpResponse, filterChain);
  }
}
