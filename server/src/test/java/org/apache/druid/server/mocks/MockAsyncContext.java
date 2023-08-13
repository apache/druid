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

import javax.servlet.AsyncContext;
import javax.servlet.AsyncListener;
import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A fake AsyncContext used in tests.  A lot of methods are implemented as
 * {@code throw new UnsupportedOperationException}, this is just an indication that nobody has needed to flesh out
 * that functionality for the mock yet and is not an indication that calling said method is a problem.  If an
 * {@code throw new UnsupportedOperationException} gets thrown out from one of these methods in a test, it is expected
 * that the developer will implement the necessary methods.
 */
public class MockAsyncContext implements AsyncContext
{
  public ServletRequest request;
  public ServletResponse response;

  private final AtomicBoolean completed = new AtomicBoolean();

  @Override
  public ServletRequest getRequest()
  {
    if (request == null) {
      throw new UnsupportedOperationException();
    } else {
      return request;
    }
  }

  @Override
  public ServletResponse getResponse()
  {
    if (response == null) {
      throw new UnsupportedOperationException();
    } else {
      return response;
    }
  }

  @Override
  public boolean hasOriginalRequestAndResponse()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dispatch()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dispatch(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dispatch(ServletContext context, String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void complete()
  {
    completed.set(true);
  }

  public boolean isCompleted()
  {
    return completed.get();
  }

  @Override
  public void start(Runnable run)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addListener(AsyncListener listener)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addListener(
      AsyncListener listener,
      ServletRequest servletRequest,
      ServletResponse servletResponse
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends AsyncListener> T createListener(Class<T> clazz)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTimeout(long timeout)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimeout()
  {
    throw new UnsupportedOperationException();
  }
}
