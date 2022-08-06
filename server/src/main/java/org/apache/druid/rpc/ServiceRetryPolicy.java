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

import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * Used by {@link ServiceClient} to decide whether to retry requests.
 */
public interface ServiceRetryPolicy
{
  int UNLIMITED = -1;

  /**
   * Returns the maximum number of desired attempts, or {@link #UNLIMITED} if unlimited. A value of 1 means no retries.
   * Zero is invalid.
   */
  long maxAttempts();

  /**
   * Returns the minimum wait time between retries.
   */
  long minWaitMillis();

  /**
   * Returns the maximum wait time between retries.
   */
  long maxWaitMillis();

  /**
   * Returns whether the given HTTP response can be retried. The response will have a non-2xx error code.
   */
  boolean retryHttpResponse(HttpResponse response);

  /**
   * Returns whether the given exception can be retried.
   */
  boolean retryThrowable(Throwable t);
}
