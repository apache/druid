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

package org.apache.druid.msq.dart.worker;

import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Retry policy for {@link DartWorkerClient}. This is a {@link StandardRetryPolicy#unlimited()} with
 * {@link #retryHttpResponse(HttpResponse)} customized to retry fewer HTTP error codes.
 */
public class DartWorkerRetryPolicy implements ServiceRetryPolicy
{
  private final boolean retryOnWorkerUnavailable;

  /**
   * Create a retry policy.
   *
   * @param retryOnWorkerUnavailable whether this policy should retry on {@link HttpResponseStatus#SERVICE_UNAVAILABLE}
   */
  public DartWorkerRetryPolicy(boolean retryOnWorkerUnavailable)
  {
    this.retryOnWorkerUnavailable = retryOnWorkerUnavailable;
  }

  @Override
  public long maxAttempts()
  {
    return StandardRetryPolicy.unlimited().maxAttempts();
  }

  @Override
  public long minWaitMillis()
  {
    return StandardRetryPolicy.unlimited().minWaitMillis();
  }

  @Override
  public long maxWaitMillis()
  {
    return StandardRetryPolicy.unlimited().maxWaitMillis();
  }

  @Override
  public boolean retryHttpResponse(HttpResponse response)
  {
    if (retryOnWorkerUnavailable) {
      return HttpResponseStatus.SERVICE_UNAVAILABLE.equals(response.getStatus());
    } else {
      return false;
    }
  }

  @Override
  public boolean retryThrowable(Throwable t)
  {
    return StandardRetryPolicy.unlimited().retryThrowable(t);
  }

  @Override
  public boolean retryLoggable()
  {
    return false;
  }

  @Override
  public boolean retryNotAvailable()
  {
    return false;
  }
}
