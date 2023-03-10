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

package org.apache.druid.server.mocks;

import org.apache.druid.java.util.common.ISE;

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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A fake HttpServletRequest used in tests.  A lot of methods are implemented as
 * {@code throw new UnsupportedOperationException}, this is just an indication that nobody has needed to flesh out
 * that functionality for the mock yet and is not an indication that calling said method is a problem.  If an
 * {@code throw new UnsupportedOperationException} gets thrown out from one of these methods in a test, it is expected
 * that the developer will implement the necessary methods.
 *
 * Also, there is a mimic method.  If you add fields to this class, be certain to adjust the mimic method as well.
 */
public class MockHttpServletRequest implements HttpServletRequest
{
  public String requestUri = null;
  public String method = null;
  public String contentType = null;
  public String remoteAddr = null;
  public String remoteHost = null;

  public LinkedHashMap<String, String> headers = new LinkedHashMap<>();
  public LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();

  public Supplier<AsyncContext> asyncContextSupplier;

  private AsyncContext currAsyncContext = null;

  @Override
  public String getAuthType()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cookie[] getCookies()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getDateHeader(String name)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getHeader(String name)
  {
    return headers.get(name);
  }

  @Override
  public Enumeration<String> getHeaders(String name)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumeration<String> getHeaderNames()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntHeader(String name)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMethod()
  {
    return unsupportedIfNull(method);
  }

  @Override
  public String getPathInfo()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPathTranslated()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getContextPath()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getQueryString()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRemoteUser()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isUserInRole(String role)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Principal getUserPrincipal()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRequestedSessionId()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRequestURI()
  {
    return unsupportedIfNull(requestUri);
  }

  @Override
  public StringBuffer getRequestURL()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getServletPath()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public HttpSession getSession(boolean create)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public HttpSession getSession()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String changeSessionId()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRequestedSessionIdValid()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRequestedSessionIdFromCookie()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRequestedSessionIdFromURL()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRequestedSessionIdFromUrl()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean authenticate(HttpServletResponse response)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void login(String username, String password)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void logout()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Part> getParts()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Part getPart(String name)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getAttribute(String name)
  {
    return attributes.get(name);
  }

  @Override
  public Enumeration<String> getAttributeNames()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCharacterEncoding()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCharacterEncoding(String env)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getContentLength()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getContentLengthLong()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getContentType()
  {
    return unsupportedIfNull(contentType);
  }

  @Override
  public ServletInputStream getInputStream()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getParameter(String name)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumeration<String> getParameterNames()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getParameterValues(String name)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String[]> getParameterMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getProtocol()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getScheme()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getServerName()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getServerPort()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferedReader getReader()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRemoteAddr()
  {
    return unsupportedIfNull(remoteAddr);
  }

  @Override
  public String getRemoteHost()
  {
    return unsupportedIfNull(remoteHost);
  }

  @Override
  public void setAttribute(String name, Object o)
  {
    attributes.put(name, o);
  }

  @Override
  public void removeAttribute(String name)
  {
    attributes.remove(name);
  }

  @Override
  public Locale getLocale()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumeration<Locale> getLocales()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSecure()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRealPath(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRemotePort()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLocalName()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLocalAddr()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLocalPort()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServletContext getServletContext()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncContext startAsync()
  {
    if (asyncContextSupplier == null) {
      throw new ISE("Start Async called, but no context supplier set.");
    } else {
      if (currAsyncContext == null) {
        currAsyncContext = asyncContextSupplier.get();
        if (currAsyncContext instanceof MockAsyncContext) {
          MockAsyncContext mocked = (MockAsyncContext) currAsyncContext;
          if (mocked.request == null) {
            mocked.request = this;
          }
        }
      }
      return currAsyncContext;
    }
  }

  @Override
  public AsyncContext startAsync(
      ServletRequest servletRequest,
      ServletResponse servletResponse
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAsyncStarted()
  {
    return currAsyncContext != null;
  }

  @Override
  public boolean isAsyncSupported()
  {
    return true;
  }

  @Override
  public AsyncContext getAsyncContext()
  {
    if (currAsyncContext == null) {
      throw new IllegalStateException("Must be put into Async mode before async context can be gottendid");
    }
    return currAsyncContext;
  }

  @Override
  public DispatcherType getDispatcherType()
  {
    throw new UnsupportedOperationException();
  }

  public void newAsyncContext(Supplier<AsyncContext> supplier)
  {
    asyncContextSupplier = supplier;
    currAsyncContext = null;
  }

  public MockHttpServletRequest mimic()
  {
    MockHttpServletRequest retVal = new MockHttpServletRequest();

    retVal.method = method;
    retVal.asyncContextSupplier = asyncContextSupplier;
    retVal.attributes.putAll(attributes);
    retVal.headers.putAll(headers);
    retVal.contentType = contentType;
    retVal.remoteAddr = remoteAddr;

    return retVal;
  }

  private <T> T unsupportedIfNull(T obj)
  {
    if (obj == null) {
      throw new UnsupportedOperationException();
    } else {
      return obj;
    }
  }
}
