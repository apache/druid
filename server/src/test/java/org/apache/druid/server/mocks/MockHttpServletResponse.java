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

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import javax.annotation.Nullable;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;

/**
 * A fake HttpServletResponse used in tests.  A lot of methods are implemented as
 * {@code throw new UnsupportedOperationException}, this is just an indication that nobody has needed to flesh out
 * that functionality for the mock yet and is not an indication that calling said method is a problem.  If an
 * {@code throw new UnsupportedOperationException} gets thrown out from one of these methods in a test, it is expected
 * that the developer will implement the necessary methods.
 */
public class MockHttpServletResponse implements HttpServletResponse
{
  public static MockHttpServletResponse forRequest(MockHttpServletRequest req)
  {
    MockHttpServletResponse response = new MockHttpServletResponse();
    req.newAsyncContext(() -> {
      final MockAsyncContext retVal = new MockAsyncContext();
      retVal.response = response;
      return retVal;
    });
    return response;
  }

  public Multimap<String, String> headers = Multimaps.newListMultimap(
      new LinkedHashMap<>(), ArrayList::new
  );

  public final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  private int statusCode;
  private String contentType;
  private String characterEncoding;

  @Override
  public void reset()
  {
    if (isCommitted()) {
      throw new IllegalStateException("Cannot reset a committed ServletResponse");
    }

    headers.clear();
    statusCode = 0;
    contentType = null;
    characterEncoding = null;
  }


  @Override
  public void addCookie(Cookie cookie)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsHeader(String name)
  {
    return headers.containsKey(name);
  }

  @Override
  public String encodeURL(String url)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String encodeRedirectURL(String url)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String encodeUrl(String url)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String encodeRedirectUrl(String url)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendError(int sc, String msg)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendError(int sc)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendRedirect(String location)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDateHeader(String name, long date)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDateHeader(String name, long date)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHeader(String name, String value)
  {
    headers.put(name, value);
  }

  @Override
  public void addHeader(String name, String value)
  {
    headers.put(name, value);
  }

  @Override
  public void setIntHeader(String name, int value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addIntHeader(String name, int value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStatus(int sc)
  {
    statusCode = sc;
  }

  @Override
  public void setStatus(int sc, String sm)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getStatus()
  {
    return statusCode;
  }

  @Nullable
  @Override
  public String getHeader(String name)
  {
    final Collection<String> vals = headers.get(name);
    if (vals == null || vals.isEmpty()) {
      return null;
    }
    return vals.iterator().next();
  }

  @Override
  public Collection<String> getHeaders(String name)
  {
    return headers.get(name);
  }

  @Override
  public Collection<String> getHeaderNames()
  {
    return headers.keySet();
  }

  @Override
  public String getCharacterEncoding()
  {
    return characterEncoding;
  }

  @Override
  public String getContentType()
  {
    return contentType;
  }

  @Override
  public ServletOutputStream getOutputStream()
  {
    return new ServletOutputStream()
    {
      @Override
      public boolean isReady()
      {
        return true;
      }

      @Override
      public void setWriteListener(WriteListener writeListener)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void write(int b)
      {
        baos.write(b);
      }

      @Override
      public void write(@NotNull byte[] b)
      {
        baos.write(b, 0, b.length);
      }

      @Override
      public void write(@NotNull byte[] b, int off, int len)
      {
        baos.write(b, off, len);
      }
    };
  }

  @Override
  public PrintWriter getWriter()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCharacterEncoding(String charset)
  {
    characterEncoding = charset;
  }

  @Override
  public void setContentLength(int len)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setContentLengthLong(long len)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setContentType(String type)
  {
    this.contentType = type;
  }

  @Override
  public void setBufferSize(int size)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBufferSize()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flushBuffer()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetBuffer()
  {
    throw new UnsupportedOperationException();
  }

  public void forceCommitted()
  {
    if (!isCommitted()) {
      baos.write(1234);
    }
  }

  @Override
  public boolean isCommitted()
  {
    return baos.size() > 0;
  }

  @Override
  public void setLocale(Locale loc)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Locale getLocale()
  {
    throw new UnsupportedOperationException();
  }
}
