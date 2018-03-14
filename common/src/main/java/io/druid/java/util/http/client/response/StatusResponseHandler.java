/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.http.client.response;

import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.nio.charset.Charset;

/**
 */
public class StatusResponseHandler implements HttpResponseHandler<StatusResponseHolder, StatusResponseHolder>
{
  private final Charset charset;

  public StatusResponseHandler(Charset charset)
  {
    this.charset = charset;
  }

  @Override
  public ClientResponse<StatusResponseHolder> handleResponse(HttpResponse response)
  {
    return ClientResponse.unfinished(
        new StatusResponseHolder(
            response.getStatus(),
            new StringBuilder(response.getContent().toString(charset))
        )
    );
  }

  @Override
  public ClientResponse<StatusResponseHolder> handleChunk(
      ClientResponse<StatusResponseHolder> response,
      HttpChunk chunk
  )
  {
    final StringBuilder builder = response.getObj().getBuilder();

    if (builder == null) {
      return ClientResponse.finished(null);
    }

    builder.append(chunk.getContent().toString(charset));
    return response;
  }

  @Override
  public ClientResponse<StatusResponseHolder> done(ClientResponse<StatusResponseHolder> response)
  {
    return ClientResponse.finished(response.getObj());
  }

  @Override
  public void exceptionCaught(
      ClientResponse<StatusResponseHolder> clientResponse, Throwable e
  )
  {
    // Its safe to Ignore as the ClientResponse returned in handleChunk were unfinished
  }

}
