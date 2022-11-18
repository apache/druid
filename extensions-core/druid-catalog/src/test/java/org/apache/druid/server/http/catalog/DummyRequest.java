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

package org.apache.druid.server.http.catalog;

import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import java.io.BufferedReader;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Test-only implementation of an HTTP request. Allows us to control
 * aspects of the request without resorting to mocks.
 */
public class DummyRequest implements HttpServletRequest
{
  public static final String GET = "GET";
  public static final String POST = "POST";
  public static final String DELETE = "DELETE";

  private final String method;
  private final Map<String, Object> attribs = new HashMap<>();
  private final String contentType;

  public DummyRequest(String method, String userName)
  {
    this(method, userName, null);
  }

  public DummyRequest(String method, String userName, String contentType)
  {
    this.method = method;
    AuthenticationResult authResult =
        new AuthenticationResult(userName, CatalogTests.TEST_AUTHORITY, null, null);
    attribs.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authResult);
    this.contentType = contentType;
  }

  public static HttpServletRequest postBy(String user)
  {
    return new DummyRequest(DummyRequest.POST, user);
  }

  public static HttpServletRequest getBy(String user)
  {
    return new DummyRequest(DummyRequest.GET, user);
  }

  public static HttpServletRequest deleteBy(String user)
  {
    return new DummyRequest(DummyRequest.DELETE, user);
  }

  @Override
  public Object getAttribute(String name)
  {
    return attribs.get(name);
  }

  @Override
  public Enumeration<String> getAttributeNames()
  {
    return null;
  }

  @Override
  public String getCharacterEncoding()
  {
    return null;
  }

  @Override
  public void setCharacterEncoding(String env)
  {
  }

  @Override
  public int getContentLength()
  {
    return 0;
  }

  @Override
  public long getContentLengthLong()
  {
    return 0;
  }

  @Override
  public String getContentType()
  {
    return contentType;
  }

  @Override
  public ServletInputStream getInputStream()
  {
    return null;
  }

  @Override
  public String getParameter(String name)
  {
    return null;
  }

  @Override
  public Enumeration<String> getParameterNames()
  {
    return null;
  }

  @Override
  public String[] getParameterValues(String name)
  {
    return null;
  }

  @Override
  public Map<String, String[]> getParameterMap()
  {
    return null;
  }

  @Override
  public String getProtocol()
  {
    return null;
  }

  @Override
  public String getScheme()
  {
    return null;
  }

  @Override
  public String getServerName()
  {
    return null;
  }

  @Override
  public int getServerPort()
  {
    return 0;
  }

  @Override
  public BufferedReader getReader()
  {
    return null;
  }

  @Override
  public String getRemoteAddr()
  {
    return null;
  }

  @Override
  public String getRemoteHost()
  {
    return null;
  }

  @Override
  public void setAttribute(String name, Object o)
  {
    attribs.put(name, o);
  }

  @Override
  public void removeAttribute(String name)
  {
  }

  @Override
  public Locale getLocale()
  {
    return null;
  }

  @Override
  public Enumeration<Locale> getLocales()
  {
    return null;
  }

  @Override
  public boolean isSecure()
  {
    return false;
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path)
  {
    return null;
  }

  @Override
  public String getRealPath(String path)
  {
    return null;
  }

  @Override
  public int getRemotePort()
  {
    return 0;
  }

  @Override
  public String getLocalName()
  {
    return null;
  }

  @Override
  public String getLocalAddr()
  {
    return null;
  }

  @Override
  public int getLocalPort()
  {
    return 0;
  }

  @Override
  public ServletContext getServletContext()
  {
    return null;
  }

  @Override
  public AsyncContext startAsync()
  {
    return null;
  }

  @Override
  public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
  {
    return null;
  }

  @Override
  public boolean isAsyncStarted()
  {
    return false;
  }

  @Override
  public boolean isAsyncSupported()
  {
    return false;
  }

  @Override
  public AsyncContext getAsyncContext()
  {
    return null;
  }

  @Override
  public DispatcherType getDispatcherType()
  {
    return null;
  }

  @Override
  public String getAuthType()
  {
    return null;
  }

  @Override
  public Cookie[] getCookies()
  {
    return null;
  }

  @Override
  public long getDateHeader(String name)
  {
    return 0;
  }

  @Override
  public String getHeader(String name)
  {
    return null;
  }

  @Override
  public Enumeration<String> getHeaders(String name)
  {
    return null;
  }

  @Override
  public Enumeration<String> getHeaderNames()
  {
    return null;
  }

  @Override
  public int getIntHeader(String name)
  {
    return 0;
  }

  @Override
  public String getMethod()
  {
    return method;
  }

  @Override
  public String getPathInfo()
  {
    return null;
  }

  @Override
  public String getPathTranslated()
  {
    return null;
  }

  @Override
  public String getContextPath()
  {
    return null;
  }

  @Override
  public String getQueryString()
  {
    return null;
  }

  @Override
  public String getRemoteUser()
  {
    return null;
  }

  @Override
  public boolean isUserInRole(String role)
  {
    return false;
  }

  @Override
  public Principal getUserPrincipal()
  {
    return null;
  }

  @Override
  public String getRequestedSessionId()
  {
    return null;
  }

  @Override
  public String getRequestURI()
  {
    return null;
  }

  @Override
  public StringBuffer getRequestURL()
  {
    return null;
  }

  @Override
  public String getServletPath()
  {
    return null;
  }

  @Override
  public HttpSession getSession(boolean create)
  {
    return null;
  }

  @Override
  public HttpSession getSession()
  {
    return null;
  }

  @Override
  public String changeSessionId()
  {
    return null;
  }

  @Override
  public boolean isRequestedSessionIdValid()
  {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromCookie()
  {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromURL()
  {
    return false;
  }

  @Override
  public boolean isRequestedSessionIdFromUrl()
  {
    return false;
  }

  @Override
  public boolean authenticate(HttpServletResponse response)
  {
    return false;
  }

  @Override
  public void login(String username, String password)
  {
  }

  @Override
  public void logout()
  {
  }

  @Override
  public Collection<Part> getParts()
  {
    return null;
  }

  @Override
  public Part getPart(String name)
  {
    return null;
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
  {
    return null;
  }
}
