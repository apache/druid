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

import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;

/**
 * Retry policy for worker-to-worker requests from {@link DartWorkerClientImpl}.
 *
 * <p>This is a {@link StandardRetryPolicy#unlimited()} with {@link #retryHttpResponse(HttpResponse)} customized
 * to retry only {@link HttpResponseStatus#SERVICE_UNAVAILABLE}. This is necessary in case worker A starts up
 * before worker B, and needs to contact it immediately.
 */
public class DartWorkerRetryPolicy implements ServiceRetryPolicy
{
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
    return HttpResponseStatus.SERVICE_UNAVAILABLE.equals(response.getStatus());
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
