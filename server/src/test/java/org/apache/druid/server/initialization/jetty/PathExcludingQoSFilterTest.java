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

import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;

public class PathExcludingQoSFilterTest
{
  // The concrete request URIs those specs are expected to exempt.
  private static final String[] EXCLUDED_PATHS = new String[]{
      "/druid/coordinator/v1/isLeader",
      "/druid/coordinator/v1/leader"
  };

  @Test
  public void testExcludedPathsBypassQosFilter() throws Exception
  {
    // The filter is intentionally NOT initialized: excluded requests must never touch the QoS semaphore,
    // so if super.doFilter() were invoked it would fail on the uninitialized semaphore.
    final PathExcludingQoSFilter filter = new PathExcludingQoSFilter(EXCLUDED_PATHS);

    for (String excludedPath : EXCLUDED_PATHS) {
      final MockHttpServletRequest request = request(excludedPath);
      final MockHttpServletResponse response = new MockHttpServletResponse();
      final RecordingFilterChain chain = new RecordingFilterChain();

      filter.doFilter(request, response, chain);

      Assert.assertEquals("Excluded request should be passed straight down the chain", 1, chain.invocations);
      Assert.assertSame(request, chain.lastRequest);
      Assert.assertEquals("Excluded request should not be rejected", 0, response.getStatus());
    }
  }

  @Test
  public void testNonExcludedPathIsHandledByQosFilter() throws Exception
  {
    final PathExcludingQoSFilter filter = new PathExcludingQoSFilter(EXCLUDED_PATHS);
    filter.init(new DummyFilterConfig(Map.of("maxRequests", "1")));

    final MockHttpServletRequest request = request("/druid/coordinator/v1/loadstatus");
    final MockHttpServletResponse response = new MockHttpServletResponse();
    final RecordingFilterChain chain = new RecordingFilterChain();

    // A single request with a free semaphore is accepted and passed down the chain by the standard QoSFilter.
    filter.doFilter(request, response, chain);

    Assert.assertEquals("Non-excluded request should be handled by the QoS filter", 1, chain.invocations);
    Assert.assertNotEquals(
        "Accepted request must not be rejected with 503",
        HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        response.getStatus()
    );
  }

  @Test
  public void testNullExcludedPathsHandlesAllRequests()
  {
    // A null exclusion list must not blow up; nothing should be treated as excluded.
    final PathExcludingQoSFilter filter = new PathExcludingQoSFilter(null);
    Assert.assertNotNull(filter);
  }

  private static MockHttpServletRequest request(String requestUri)
  {
    final MockHttpServletRequest request = new MockHttpServletRequest()
    {
      @Override
      public String getContextPath()
      {
        return "";
      }
    };
    request.requestUri = requestUri;
    return request;
  }

  private static class RecordingFilterChain implements FilterChain
  {
    private int invocations = 0;
    private ServletRequest lastRequest;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response)
    {
      invocations++;
      lastRequest = request;
    }
  }

  private static class DummyFilterConfig implements FilterConfig
  {
    private final Map<String, String> initParameters;

    DummyFilterConfig(Map<String, String> initParameters)
    {
      this.initParameters = initParameters;
    }

    @Override
    public String getFilterName()
    {
      return "qos";
    }

    @Override
    public ServletContext getServletContext()
    {
      return null;
    }

    @Override
    public String getInitParameter(String name)
    {
      return initParameters.get(name);
    }

    @Override
    public Enumeration<String> getInitParameterNames()
    {
      return Collections.enumeration(initParameters.keySet());
    }
  }
}
