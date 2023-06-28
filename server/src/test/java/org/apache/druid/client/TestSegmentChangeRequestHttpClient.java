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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.java.util.common.ISE;
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

  private int requestCount = 0;

  public void failNextRequestOnClientWith(RuntimeException clientError)
  {
    results.add(new ResultHolder<>(null, null, clientError, null));
  }

  public void failNextRequestOnServerWith(DruidException serverError)
  {
    results.add(new ResultHolder<>(null, null, null, serverError));
  }

  public <T> void completeNextRequestWith(T result, TypeReference<T> typeReference)
  {
    results.add(new ResultHolder<>(() -> result, typeReference, null, null));
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
    final int currentRequest = requestCount++;

    final ResultHolder<?> nextResult = results.poll();
    if (nextResult == null) {
      throw new ISE("No known response for request [%d]", currentRequest);
    } else if (nextResult.clientError != null) {
      throw nextResult.clientError;
    } else if (nextResult.serverError != null) {
      HttpResponse errorResponse = buildErrorResponse(nextResult.serverError);
      httpResponseHandler.handleResponse(errorResponse, null);
      return (ListenableFuture<Final>) Futures.immediateFuture(new ByteArrayInputStream(new byte[0]));
    } else {
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      httpResponse.setContent(ChannelBuffers.buffer(0));
      httpResponseHandler.handleResponse(httpResponse, null);
    }

    try {
      ByteArrayInputStream resultBytes = new ByteArrayInputStream(
          MAPPER.writerFor(nextResult.typeReference)
                .writeValueAsBytes(nextResult.supplier.get())
      );
      return (ListenableFuture<Final>) Futures.immediateFuture(resultBytes);
    }
    catch (Exception e) {
      throw new RE(e, "Error while sending HTTP response: %s", e.getMessage());
    }
  }

  private HttpResponse buildErrorResponse(DruidException druidException)
  {
    HttpResponse httpResponse = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.valueOf(druidException.getStatusCode())
    );
    httpResponse.setContent(ChannelBuffers.buffer(0));

    ErrorResponse errorResponse = druidException.toErrorResponse();
    try {
      httpResponse.setContent(ChannelBuffers.copiedBuffer(MAPPER.writeValueAsBytes(errorResponse)));
      return httpResponse;
    }
    catch (JsonProcessingException e) {
      throw new ISE("Error while serializing given response");
    }
  }

  private static class ResultHolder<R>
  {
    final Supplier<R> supplier;
    final TypeReference<R> typeReference;

    final RuntimeException clientError;
    final DruidException serverError;

    ResultHolder(
        Supplier<R> supplier,
        TypeReference<R> typeReference,
        RuntimeException clientError,
        DruidException serverError
    )
    {
      this.supplier = supplier;
      this.typeReference = typeReference;
      this.clientError = clientError;
      this.serverError = serverError;
    }
  }
}
