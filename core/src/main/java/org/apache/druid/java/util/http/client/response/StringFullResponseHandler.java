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

package org.apache.druid.java.util.http.client.response;

import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.nio.charset.Charset;

/**
 * {@link HttpResponseHandler} for stream data of string type.
 * The stream data is appended to {@link StringFullResponseHolder} whenever {@link #handleChunk} is called.
 */
public class StringFullResponseHandler
    implements HttpResponseHandler<StringFullResponseHolder, StringFullResponseHolder>
{
  private final Charset charset;

  public StringFullResponseHandler(Charset charset)
  {
    this.charset = charset;
  }

  @Override
  public ClientResponse<StringFullResponseHolder> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    return ClientResponse.unfinished(new StringFullResponseHolder(response.getStatus(), response, charset));
  }

  @Override
  public ClientResponse<StringFullResponseHolder> handleChunk(
      ClientResponse<StringFullResponseHolder> response,
      HttpChunk chunk,
      long chunkNum
  )
  {
    final StringFullResponseHolder holder = response.getObj();

    if (holder == null) {
      return ClientResponse.finished(null);
    }

    holder.addChunk(chunk.getContent().toString(charset));
    return response;
  }

  @Override
  public ClientResponse<StringFullResponseHolder> done(ClientResponse<StringFullResponseHolder> response)
  {
    return ClientResponse.finished(response.getObj());
  }

  @Override
  public void exceptionCaught(ClientResponse<StringFullResponseHolder> clientResponse, Throwable e)
  {
    // Its safe to Ignore as the ClientResponse returned in handleChunk were unfinished
  }

}
