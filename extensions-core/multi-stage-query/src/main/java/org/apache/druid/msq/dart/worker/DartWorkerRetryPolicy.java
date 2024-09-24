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

import org.apache.druid.messages.client.MessageListener;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * Retry policy for {@link DartWorkerClient}. This is a {@link StandardRetryPolicy#unlimitedWithoutRetryLogging()}
 * with an adjustment to retry 404, which can happen when the worker we're trying to contact hasn't been initialized
 * yet.
 */
public class DartWorkerRetryPolicy implements ServiceRetryPolicy
{
  public static final DartWorkerRetryPolicy INSTANCE = new DartWorkerRetryPolicy();

  /**
   * Must be an unlimited policy to avoid zombie workers; see note in {@link DartWorkerResource#httpPostStopWorker}.
   *
   * If the worker we're trying to contact has gone, we'll notice that through either
   * {@link MessageListener#serverRemoved(DruidNode)} (if we're a controller) or by receiving a stop command from
   * a controller (if we're a worker).
   */
  private static final ServiceRetryPolicy DELEGATE = StandardRetryPolicy.unlimitedWithoutRetryLogging();

  private DartWorkerRetryPolicy()
  {
    // Singleton.
  }

  @Override
  public long maxAttempts()
  {
    return DELEGATE.maxAttempts();
  }

  @Override
  public long minWaitMillis()
  {
    return DELEGATE.minWaitMillis();
  }

  @Override
  public long maxWaitMillis()
  {
    return DELEGATE.maxWaitMillis();
  }

  @Override
  public boolean retryHttpResponse(HttpResponse response)
  {
    // Retry 404, which can happen when the worker we're trying to contact hasn't been initialized yet.
    return response.getStatus().getCode() == 404 || DELEGATE.retryHttpResponse(response);
  }

  @Override
  public boolean retryThrowable(Throwable t)
  {
    return DELEGATE.retryThrowable(t);
  }

  @Override
  public boolean retryLoggable()
  {
    return DELEGATE.retryLoggable();
  }

  @Override
  public boolean retryNotAvailable()
  {
    return DELEGATE.retryNotAvailable();
  }
}
