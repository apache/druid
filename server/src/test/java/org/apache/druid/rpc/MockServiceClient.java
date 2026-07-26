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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
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
      final HttpResponse fullResponse = expectation.response.valueOrThrow();

      // Mimic the real Netty pipeline (HttpClientCodec with no aggregator): headers arrive via handleResponse
      // as a plain HttpResponse, and the body arrives separately as content chunks. Delivering the body only
      // through handleChunk avoids double-counting for handlers that also read content in handleResponse.
      final HttpResponse headerResponse = new DefaultHttpResponse(fullResponse.protocolVersion(), fullResponse.status());
      headerResponse.headers().set(fullResponse.headers());

      ClientResponse<IntermediateType> intermediate = handler.handleResponse(headerResponse, TestHttpClient.NOOP_TRAFFIC_COP);
      final ByteBuf content =
          fullResponse instanceof HttpContent ? ((HttpContent) fullResponse).content() : Unpooled.EMPTY_BUFFER;
      intermediate = handler.handleChunk(intermediate, new DefaultLastHttpContent(content), 0);

      final ClientResponse<FinalType> response = handler.done(intermediate);
      return Futures.immediateFuture(response.getObj());
    } else {
      return Futures.immediateFailedFuture(expectation.response.error());
    }
  }

  @Override
  public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
  {
    return this;
  }

  public MockServiceClient expectAndRespond(final RequestBuilder request, final HttpResponse response)
  {
    expectations.add(new Expectation(request, Either.value(response)));
    return this;
  }

  public MockServiceClient expectAndRespond(
      final RequestBuilder request,
      final HttpResponseStatus status,
      final Map<String, String> headers,
      final byte[] content
  )
  {
    final HttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        status,
        content == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(content)
    );
    for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
      response.headers().set(headerEntry.getKey(), headerEntry.getValue());
    }
    return expectAndRespond(request, response);
  }

  public MockServiceClient expectAndThrow(final RequestBuilder request, final Throwable e)
  {
    expectations.add(new Expectation(request, Either.error(e)));
    return this;
  }

  public void verify()
  {
    Assert.assertTrue("all requests were made", expectations.isEmpty());
  }

  private static class Expectation
  {
    private final RequestBuilder request;
    private final Either<Throwable, HttpResponse> response;

    public Expectation(RequestBuilder request, Either<Throwable, HttpResponse> response)
    {
      this.request = request;
      this.response = response;
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
