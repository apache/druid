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

package org.apache.druid.server.security;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class AllowHttpMethodsResourceFilterTest
{
  private static final String METHOD_ALLOWED = "METHOD_ALLOWED";
  private static final String METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED";

  @Mock
  private HttpServletRequest request;
  @Mock
  private HttpServletResponse response;
  @Mock
  private FilterChain filterChain;

  private AllowHttpMethodsResourceFilter target;

  @Before
  public void setUp()
  {
    target = new AllowHttpMethodsResourceFilter(ImmutableList.of(METHOD_ALLOWED));
  }

  @Test
  public void testDoFilterMethodNotAllowedShouldReturnAnError() throws IOException, ServletException
  {
    Mockito.doReturn(METHOD_NOT_ALLOWED).when(request).getMethod();
    target.doFilter(request, response, filterChain);
    Mockito.verify(response).sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
  }

  @Test
  public void testDoFilterMethodAllowedShouldFilter() throws IOException, ServletException
  {
    Mockito.doReturn(METHOD_ALLOWED).when(request).getMethod();
    target.doFilter(request, response, filterChain);
    Mockito.verify(filterChain).doFilter(request, response);
  }

  @Test
  public void testDoFilterSupportedHttpMethodsShouldFilter() throws IOException, ServletException
  {
    for (String method : AllowHttpMethodsResourceFilter.SUPPORTED_METHODS) {
      Mockito.doReturn(method).when(request).getMethod();
      target.doFilter(request, response, filterChain);
      Mockito.verify(filterChain).doFilter(request, response);
      Mockito.clearInvocations(filterChain);
    }
  }
}
