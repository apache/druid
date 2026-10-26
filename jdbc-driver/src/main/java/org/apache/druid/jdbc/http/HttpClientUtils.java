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

package org.apache.druid.jdbc.http;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.http.HttpClient;

/**
 * Utility class for closing {@link HttpClient}. Necessary because this driver is built for Java 17, where clients
 * cannot be closed, but we still want to properly close them on Java 21+ (where they are AutoCloseable).
 */
public class HttpClientUtils
{
  @Nullable
  private static final Method HTTP_CLIENT_CLOSE = findHttpClientMethod("close");

  @Nullable
  private static final Method HTTP_CLIENT_SHUTDOWN_NOW = findHttpClientMethod("shutdownNow");

  private HttpClientUtils()
  {
    // No instantiation.
  }

  /**
   * Shuts down a client immediately, if possible. On Java 17, where these methods on {@link HttpClient} do not exist,
   * this is a no-op.
   */
  public static void close(final HttpClient httpClient) throws IOException
  {
    invokeIfPresent(HTTP_CLIENT_SHUTDOWN_NOW, httpClient);
    invokeIfPresent(HTTP_CLIENT_CLOSE, httpClient);
  }

  private static void invokeIfPresent(@Nullable final Method method, final HttpClient httpClient) throws IOException
  {
    if (method != null) {
      try {
        method.invoke(httpClient);
      }
      catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      catch (InvocationTargetException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof IOException ioe) {
          throw ioe;
        } else if (cause instanceof RuntimeException re) {
          throw re;
        } else if (cause instanceof Error error) {
          throw error;
        } else {
          throw new RuntimeException(cause);
        }
      }
    }
  }

  /**
   * Returns the named no-arg {@link HttpClient} method if the current JVM has it, or null if it does not.
   */
  @Nullable
  private static Method findHttpClientMethod(final String name)
  {
    try {
      return HttpClient.class.getMethod(name);
    }
    catch (NoSuchMethodException e) {
      return null;
    }
  }
}
