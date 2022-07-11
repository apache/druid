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

package org.apache.druid.rpc;

import org.apache.druid.java.util.common.IAE;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;

/**
 * Retry policy configurable with a maximum number of attempts and min/max wait time.
 *
 * The policy retries on IOExceptions and ChannelExceptions, and on HTTP 500, 502, 503, and 504. Other exceptions
 * and other HTTP status codes are considered nonretryable errors.
 */
public class StandardRetryPolicy implements ServiceRetryPolicy
{
  private static final long DEFAULT_MIN_WAIT_MS = 100;
  private static final long DEFAULT_MAX_WAIT_MS = 30_000;

  private static final StandardRetryPolicy DEFAULT_UNLIMITED_POLICY = new Builder().maxAttempts(UNLIMITED).build();
  private static final StandardRetryPolicy DEFAULT_NO_RETRIES_POLICY = new Builder().maxAttempts(1).build();

  private final long maxAttempts;
  private final long minWaitMillis;
  private final long maxWaitMillis;

  private StandardRetryPolicy(long maxAttempts, long minWaitMillis, long maxWaitMillis)
  {
    this.maxAttempts = maxAttempts;
    this.minWaitMillis = minWaitMillis;
    this.maxWaitMillis = maxWaitMillis;

    if (maxAttempts == 0) {
      throw new IAE("maxAttempts must be positive (limited) or negative (unlimited); cannot be zero.");
    }
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static StandardRetryPolicy unlimited()
  {
    return DEFAULT_UNLIMITED_POLICY;
  }

  public static StandardRetryPolicy noRetries()
  {
    return DEFAULT_NO_RETRIES_POLICY;
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
  public boolean retryHttpResponse(final HttpResponse response)
  {
    final int code = response.getStatus().getCode();

    return code == HttpResponseStatus.BAD_GATEWAY.getCode()
           || code == HttpResponseStatus.SERVICE_UNAVAILABLE.getCode()
           || code == HttpResponseStatus.GATEWAY_TIMEOUT.getCode()

           // Technically shouldn't retry this last one, but servers sometimes return HTTP 500 for retryable errors.
           || code == HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode();
  }

  @Override
  public boolean retryThrowable(Throwable t)
  {
    return t instanceof IOException
           || t instanceof ChannelException
           || (t.getCause() != null && retryThrowable(t.getCause()));
  }

  public static class Builder
  {
    private long maxAttempts = 0; // Zero is an invalid value: so, this parameter must be explicitly specified
    private long minWaitMillis = DEFAULT_MIN_WAIT_MS;
    private long maxWaitMillis = DEFAULT_MAX_WAIT_MS;

    public Builder maxAttempts(final long maxAttempts)
    {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder minWaitMillis(final long minWaitMillis)
    {
      this.minWaitMillis = minWaitMillis;
      return this;
    }

    public Builder maxWaitMillis(final long maxWaitMillis)
    {
      this.maxWaitMillis = maxWaitMillis;
      return this;
    }

    public StandardRetryPolicy build()
    {
      return new StandardRetryPolicy(maxAttempts, minWaitMillis, maxWaitMillis);
    }
  }
}
