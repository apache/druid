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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.segment.TestHelper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;

import java.io.ByteArrayInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class TestSegmentChangeRequestHttpClient implements HttpClient
{
  private final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private final BlockingQueue<ResultHolder<?>> results = new LinkedBlockingQueue<>();

  public void addNextError(RuntimeException error)
  {
    results.add(new ResultHolder<>(null, null, error));
  }

  public <T> void addNextResult(T result, TypeReference<T> typeReference)
  {
    results.add(new ResultHolder<>(() -> result, typeReference, null));
  }

  public boolean hasPendingResults()
  {
    return !results.isEmpty();
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> httpResponseHandler
  )
  {
    throw new UnsupportedOperationException("Not Implemented.");
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> httpResponseHandler,
      Duration duration
  )
  {
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    httpResponse.setContent(ChannelBuffers.buffer(0));
    httpResponseHandler.handleResponse(httpResponse, null);

    try {
      ResultHolder<?> nextResult = results.take();
      if (nextResult.error != null) {
        throw nextResult.error;
      }

      ByteArrayInputStream resultBytes = new ByteArrayInputStream(
          MAPPER.writerFor(nextResult.typeReference)
                .writeValueAsBytes(nextResult.get())
      );
      return (ListenableFuture<Final>) Futures.immediateFuture(resultBytes);
    }
    catch (Exception e) {
      throw new RE(e, "Error while sending HTTP response: %s", e.getMessage());
    }
  }

  private static class ResultHolder<R>
  {
    final Supplier<R> resultSupplier;
    final TypeReference<R> typeReference;
    final RuntimeException error;

    ResultHolder(Supplier<R> resultSupplier, TypeReference<R> typeReference, RuntimeException error)
    {
      this.resultSupplier = resultSupplier;
      this.typeReference = typeReference;
      this.error = error;
    }

    R get()
    {
      return resultSupplier.get();
    }
  }
}
