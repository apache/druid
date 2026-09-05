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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

@ExtendWith(MockitoExtension.class)
public class RedirectFilterTest
{
  private static final String REQUEST_URI = "/druid/coordinator/v1/loadstatus";
  private static final String QUERY_STRING = "simple=true";

  @Mock
  private RedirectInfo redirectInfo;
  @Mock
  private HttpServletRequest request;
  @Mock
  private HttpServletResponse response;
  @Mock
  private FilterChain filterChain;

  private RedirectFilter redirectFilter;

  @BeforeEach
  public void setUp()
  {
    redirectFilter = new RedirectFilter(redirectInfo);
    Mockito.when(request.getRequestURI()).thenReturn(REQUEST_URI);
    Mockito.when(request.getQueryString()).thenReturn(QUERY_STRING);
    Mockito.when(redirectInfo.doLocal(REQUEST_URI)).thenReturn(false);
  }

  @Test
  public void testRedirect() throws Exception
  {
    final String location = "https://leader.example:8081" + REQUEST_URI + "?" + QUERY_STRING + "#result";
    Mockito.when(redirectInfo.getRedirectURL(QUERY_STRING, REQUEST_URI)).thenReturn(URI.create(location).toURL());

    redirectFilter.doFilter(request, response, filterChain);

    Mockito.verify(response).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
    Mockito.verify(response).setHeader("Location", location);
    Mockito.verifyNoInteractions(filterChain);
  }

  @Test
  public void testRedirectPreservesEncodedNewlines() throws Exception
  {
    final String location = "https://leader.example/path%0D%0Avalue?query=%0a";
    Mockito.when(redirectInfo.getRedirectURL(QUERY_STRING, REQUEST_URI)).thenReturn(URI.create(location).toURL());

    redirectFilter.doFilter(request, response, filterChain);

    Mockito.verify(response).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
    Mockito.verify(response).setHeader("Location", location);
    Mockito.verifyNoInteractions(filterChain);
  }

  @Test
  public void testRedirectRejectsCarriageReturn() throws Exception
  {
    assertInvalidRedirect("https://leader.example/path\rInjected: value");
  }

  @Test
  public void testRedirectRejectsLineFeed() throws Exception
  {
    assertInvalidRedirect("https://leader.example/path\nInjected: value");
  }

  private void assertInvalidRedirect(String location) throws Exception
  {
    Mockito.when(redirectInfo.getRedirectURL(QUERY_STRING, REQUEST_URI)).thenReturn(urlWithExternalForm(location));

    redirectFilter.doFilter(request, response, filterChain);

    Mockito.verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);
    Mockito.verify(response, Mockito.never()).setStatus(Mockito.anyInt());
    Mockito.verify(response, Mockito.never()).setHeader(Mockito.anyString(), Mockito.anyString());
    Mockito.verifyNoInteractions(filterChain);
  }

  private static URL urlWithExternalForm(final String externalForm) throws Exception
  {
    return URL.of(
        URI.create("https://leader.example"),
        new URLStreamHandler()
        {
          @Override
          protected URLConnection openConnection(final URL url)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          protected String toExternalForm(final URL url)
          {
            return externalForm;
          }
        }
    );
  }
}
