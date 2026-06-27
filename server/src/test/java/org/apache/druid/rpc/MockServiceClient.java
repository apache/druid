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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.client.TestHttpClient;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.junit.Assert;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mock implementation of {@link ServiceClient}.
 */
public class MockServiceClient implements ServiceClient
{
  private final Queue<Expectation> expectations = new ArrayDeque<>(16);
  private int requestNumber = -1;

  @Override
  public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
      final RequestBuilder requestBuilder,
      final HttpResponseHandler<IntermediateType, FinalType> handler
  )
  {
    final Expectation expectation = expectations.poll();

    requestNumber++;
    Assert.assertEquals(
        "request[" + requestNumber + "]",
        expectation == null ? null : expectation.request,
        requestBuilder
    );

    if (expectation.response.isValue()) {
      ClientResponse<IntermediateType> interm =
          handler.handleResponse(expectation.response.valueOrThrow(), TestHttpClient.NOOP_TRAFFIC_COP);
      // Netty 4: body content arrives via HttpContent chunks after the initial HttpResponse.
      if (expectation.content != null) {
        interm = handler.handleChunk(
            interm,
            new DefaultHttpContent(Unpooled.wrappedBuffer(expectation.content)),
            1
        );
      }
      final ClientResponse<FinalType> response = handler.done(interm);
      return Futures.immediateFuture(response.getObj());
    } else {
      final Throwable error = expectation.response.error();
      if (error instanceof BlockingSentinel) {
        try {
          Thread.sleep(10_000);
          return Futures.immediateFailedFuture(new RuntimeException("expected interruption did not happen"));
        }
        catch (InterruptedException e) {
          ((BlockingSentinel) error).interruptedFlag.set(true);
          Thread.currentThread().interrupt();
          return Futures.immediateFailedFuture(e);
        }
      }
      return Futures.immediateFailedFuture(error);
    }
  }

  @Override
  public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
  {
    return this;
  }

  public MockServiceClient expectAndRespond(final RequestBuilder request, final HttpResponse response)
  {
    expectations.add(new Expectation(request, Either.value(response), null));
    return this;
  }

  public MockServiceClient expectAndRespond(
      final RequestBuilder request,
      final HttpResponseStatus status,
      final Map<String, String> headers,
      final byte[] content
  )
  {
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
      response.headers().set(headerEntry.getKey(), headerEntry.getValue());
    }
    expectations.add(new Expectation(request, Either.value(response), content));
    return this;
  }

  public MockServiceClient expectAndThrow(final RequestBuilder request, final Throwable e)
  {
    expectations.add(new Expectation(request, Either.error(e), null));
    return this;
  }

  /**
   * Sleep on the request thread until interrupted; sets {@code interruptedFlag} when the thread is interrupted.
   * Replaces the Netty-3 idiom of overriding {@code HttpResponse.getContent()} to block, which is no longer
   * applicable in Netty 4 where the initial {@link HttpResponse} carries no body.
   */
  public MockServiceClient expectAndBlock(final RequestBuilder request, final AtomicBoolean interruptedFlag)
  {
    expectations.add(new Expectation(request, Either.error(new BlockingSentinel(interruptedFlag)), null));
    return this;
  }

  private static class BlockingSentinel extends RuntimeException
  {
    private final AtomicBoolean interruptedFlag;

    BlockingSentinel(AtomicBoolean interruptedFlag)
    {
      this.interruptedFlag = interruptedFlag;
    }
  }

  public void verify()
  {
    Assert.assertTrue("all requests were made", expectations.isEmpty());
  }

  private static class Expectation
  {
    private final RequestBuilder request;
    private final Either<Throwable, HttpResponse> response;
    private final byte[] content;

    public Expectation(RequestBuilder request, Either<Throwable, HttpResponse> response, byte[] content)
    {
      this.request = request;
      this.response = response;
      this.content = content;
    }

    @Override
    public String toString()
    {
      return "Expectation{" +
             "request=" + request +
             ", response=" + response +
             '}';
    }
  }
}
