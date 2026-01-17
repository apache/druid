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

package org.apache.druid.msq.indexing;

import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.util.concurrent.ExecutionException;

/**
 * Retry policy for {@link IndexerDataServerQueryHandler}.
 */
public class IndexerDataServerRetryPolicy implements ServiceRetryPolicy
{
  private static final long DEFAULT_MAX_ATTEMPTS = 5;
  private static final long DEFAULT_MIN_WAIT_MILLIS = RetryUtils.BASE_SLEEP_MILLIS;
  private static final long DEFAULT_MAX_WAIT_MILLIS = RetryUtils.MAX_SLEEP_MILLIS;

  private final long maxAttempts;
  private final long minWaitMillis;
  private final long maxWaitMillis;

  public IndexerDataServerRetryPolicy(long maxAttempts, long minWaitMillis, long maxWaitMillis)
  {
    this.maxAttempts = maxAttempts;
    this.minWaitMillis = minWaitMillis;
    this.maxWaitMillis = maxWaitMillis;
  }

  /**
   * Default policy for production.
   */
  public static IndexerDataServerRetryPolicy standard()
  {
    return new IndexerDataServerRetryPolicy(DEFAULT_MAX_ATTEMPTS, DEFAULT_MIN_WAIT_MILLIS, DEFAULT_MAX_WAIT_MILLIS);
  }

  /**
   * Policy that does not retry.
   */
  public static IndexerDataServerRetryPolicy noRetries()
  {
    return new IndexerDataServerRetryPolicy(1, 0, 0);
  }

  @Override
  public long maxAttempts()
  {
    return maxAttempts;
  }

  @Override
  public long minWaitMillis()
  {
    return minWaitMillis;
  }

  @Override
  public long maxWaitMillis()
  {
    return maxWaitMillis;
  }

  @Override
  public boolean retryHttpResponse(HttpResponse response)
  {
    return false;
  }

  @Override
  public boolean retryThrowable(Throwable t)
  {
    // Retry on all exceptions, except when the exception chain indicates explicit interruption.
    return !(t instanceof ExecutionException
             && t.getCause() instanceof QueryInterruptedException
             && t.getCause().getCause() instanceof InterruptedException);
  }

  @Override
  public boolean retryLoggable()
  {
    return true;
  }

  @Override
  public boolean retryNotAvailable()
  {
    return false;
  }
}
