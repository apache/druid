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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.rpc.indexing.OverlordClient;

import java.util.concurrent.ExecutionException;

/**
 * Mid-level client that provides an API similar to low-level {@link HttpClient}, but accepts {@link RequestBuilder}
 * instead of {@link org.apache.druid.java.util.http.client.Request}, and internally handles service location
 * and retries.
 *
 * In most cases, this client is further wrapped in a high-level client like
 * {@link OverlordClient}.
 */
public interface ServiceClient
{
  long MAX_REDIRECTS = 3;

  /**
   * Perform a request asynchronously.
   *
   * Unlike {@link HttpClient#go}, the provided "handler" is only used for 2xx responses.
   *
   * Response codes 1xx, 4xx, and 5xx are retried with backoff according to the client's {@link ServiceRetryPolicy}.
   * If attempts are exhausted, the future will fail with {@link RpcException} containing the most recently
   * encountered error.
   *
   * Redirects from 3xx responses are followed up to a chain length of {@link #MAX_REDIRECTS} and do not consume
   * attempts. Redirects are validated against the targets returned by {@link ServiceLocator}: the client will not
   * follow a redirect to a target that does not appear in the returned {@link ServiceLocations}.
   *
   * If the service is unavailable at the time an attempt is made -- i.e. if {@link ServiceLocator#locate()} returns an
   * empty set -- then an attempt is consumed and the client will try to locate the service again on the next attempt.
   *
   * If an exception occurs midstream after an OK HTTP response (2xx) then the behavior depends on the handler. If
   * the handler has not yet returned a finished object, the service client will automatically retry based on the
   * provided {@link ServiceRetryPolicy}. On the other hand, if the handler has returned a finished object, the
   * service client will not retry. Behavior in this case is up to the caller, who will have already received the
   * finished object as the future's resolved value.
   *
   * Resolves to {@link HttpResponseException} if the final attempt failed due to a non-OK HTTP server response.
   *
   * Resolves to {@link ServiceNotAvailableException} if the final attempt failed because the service was not
   * available (i.e. if the locator returned an empty set of locations).
   *
   * Resolves to {@link ServiceClosedException} if the final attempt failed because the service was closed. This is
   * different from not available: generally, "not available" is a temporary condition whereas "closed" is a
   * permanent condition.
   */
  <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
      RequestBuilder requestBuilder,
      HttpResponseHandler<IntermediateType, FinalType> handler
  );

  /**
   * Perform a request synchronously.
   *
   * Same behavior as {@link #asyncRequest}, except the result is returned synchronously. Any exceptions from the
   * underlying service call are wrapped in an ExecutionException.
   */
  default <IntermediateType, FinalType> FinalType request(
      RequestBuilder requestBuilder,
      HttpResponseHandler<IntermediateType, FinalType> handler
  ) throws InterruptedException, ExecutionException
  {
    // Cancel the future if we are interrupted. Nobody else is waiting for it.
    return FutureUtils.get(asyncRequest(requestBuilder, handler), true);
  }

  /**
   * Returns a copy of this client with a different {@link ServiceRetryPolicy}.
   */
  ServiceClient withRetryPolicy(ServiceRetryPolicy retryPolicy);
}
