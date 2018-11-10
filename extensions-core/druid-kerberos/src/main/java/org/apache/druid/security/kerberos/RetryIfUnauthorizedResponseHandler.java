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

package org.apache.druid.security.kerberos;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

public class RetryIfUnauthorizedResponseHandler<Intermediate, Final>
    implements HttpResponseHandler<RetryResponseHolder<Intermediate>, RetryResponseHolder<Final>>
{
  private static final Logger log = new Logger(RetryIfUnauthorizedResponseHandler.class);
  private final HttpResponseHandler<Intermediate, Final> httpResponseHandler;


  public RetryIfUnauthorizedResponseHandler(HttpResponseHandler<Intermediate, Final> httpResponseHandler)
  {
    this.httpResponseHandler = httpResponseHandler;
  }

  @Override
  public ClientResponse<RetryResponseHolder<Intermediate>> handleResponse(
      HttpResponse httpResponse,
      TrafficCop trafficCop
  )
  {
    log.debug("UnauthorizedResponseHandler - Got response status [%s]", httpResponse.getStatus());
    if (httpResponse.getStatus().equals(HttpResponseStatus.UNAUTHORIZED)) {
      // Drain the buffer
      httpResponse.getContent().toString();
      return ClientResponse.unfinished(RetryResponseHolder.retry());
    } else {
      return wrap(httpResponseHandler.handleResponse(httpResponse, trafficCop));
    }
  }

  @Override
  public ClientResponse<RetryResponseHolder<Intermediate>> handleChunk(
      ClientResponse<RetryResponseHolder<Intermediate>> clientResponse,
      HttpChunk httpChunk,
      long chunkNum
  )
  {
    if (clientResponse.getObj().shouldRetry()) {
      httpChunk.getContent().toString();
      return clientResponse;
    } else {
      return wrap(httpResponseHandler.handleChunk(unwrap(clientResponse), httpChunk, chunkNum));
    }
  }

  @Override
  public ClientResponse<RetryResponseHolder<Final>> done(ClientResponse<RetryResponseHolder<Intermediate>> clientResponse)
  {
    if (clientResponse.getObj().shouldRetry()) {
      return ClientResponse.finished(RetryResponseHolder.retry());
    } else {
      return wrap(httpResponseHandler.done(unwrap(clientResponse)));
    }
  }

  @Override
  public void exceptionCaught(ClientResponse<RetryResponseHolder<Intermediate>> clientResponse, Throwable throwable)
  {
    httpResponseHandler.exceptionCaught(unwrap(clientResponse), throwable);
  }

  private <T> ClientResponse<RetryResponseHolder<T>> wrap(ClientResponse<T> response)
  {
    if (response.isFinished()) {
      return ClientResponse.finished(new RetryResponseHolder<>(false, response.getObj()));
    } else {
      return ClientResponse.unfinished(
          new RetryResponseHolder<>(false, response.getObj()),
          response.isContinueReading()
      );
    }
  }

  private <T> ClientResponse<T> unwrap(ClientResponse<RetryResponseHolder<T>> response)
  {
    if (response.isFinished()) {
      return ClientResponse.finished(response.getObj().getObj());
    } else {
      return ClientResponse.unfinished(response.getObj().getObj(), response.isContinueReading());
    }
  }


}
