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

package org.apache.druid.rpc.handler;

import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;

/**
 * Response handler that deserializes responses as JSON or Smile.
 *
 * This handler returns a finished object only when the response has been fully read. Until then, it buffers up
 * the received JSON or Smile in memory.
 *
 * This handler does not look at the HTTP code; it tries to deserialize any response with the proper content type.
 * If you don't want this behavior, consider wrapping the handler in
 * {@link org.apache.druid.java.util.http.client.response.ObjectOrErrorResponseHandler} so it will only be called
 * for 2xx responses. Alternatively, consider using this handler through
 * {@link org.apache.druid.rpc.ServiceClient#asyncRequest} or
 * {@link org.apache.druid.rpc.ServiceClient#request}, which only call provided handlers for 2xx responses.
 */
public class ObjectMapperHttpResponseHandler<T>
    implements HttpResponseHandler<BytesFullResponseHolder, T>
{
  private static final String CONTENT_TYPE_HEADER = "content-type";

  private final DeserializeFn<T> deserializeFn;
  private final String contentType;
  private final BytesFullResponseHandler delegate;

  protected ObjectMapperHttpResponseHandler(final DeserializeFn<T> deserializeFn, final String contentType)
  {
    this.deserializeFn = deserializeFn;
    this.contentType = contentType;
    this.delegate = new BytesFullResponseHandler();
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> handleResponse(
      final HttpResponse response,
      final TrafficCop trafficCop
  )
  {
    return delegate.handleResponse(response, trafficCop);
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> handleChunk(
      final ClientResponse<BytesFullResponseHolder> clientResponse,
      final HttpChunk chunk,
      final long chunkNum
  )
  {
    return delegate.handleChunk(clientResponse, chunk, chunkNum);
  }

  @Override
  public ClientResponse<T> done(final ClientResponse<BytesFullResponseHolder> clientResponse)
  {
    final ClientResponse<BytesFullResponseHolder> delegateResponse = delegate.done(clientResponse);
    final String responseContentType = delegateResponse.getObj().getResponse().headers().get(CONTENT_TYPE_HEADER);

    if (contentType.equals(responseContentType)) {
      try {
        return ClientResponse.finished(deserializeFn.deserialize(delegateResponse.getObj().getContent()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RE("Unexpected Content-Type: expected [%s] but received [%s]", contentType, responseContentType);
    }
  }

  @Override
  public void exceptionCaught(final ClientResponse<BytesFullResponseHolder> clientResponse, final Throwable e)
  {
    delegate.exceptionCaught(clientResponse, e);
  }

  protected interface DeserializeFn<T>
  {
    T deserialize(byte[] in) throws IOException;
  }
}
