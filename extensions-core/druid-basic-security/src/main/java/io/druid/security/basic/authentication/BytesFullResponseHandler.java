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

package io.druid.security.basic.authentication;

import io.druid.java.util.http.client.response.ClientResponse;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

public class BytesFullResponseHandler implements HttpResponseHandler<FullResponseHolder, FullResponseHolder>
{
  @Override
  public ClientResponse<FullResponseHolder> handleResponse(HttpResponse response)
  {
    BytesFullResponseHolder holder = new BytesFullResponseHolder(
        response.getStatus(),
        response,
        null
    );

    holder.addChunk(getContentBytes(response.getContent()));

    return ClientResponse.unfinished(
        holder
    );
  }

  @Override
  public ClientResponse<FullResponseHolder> handleChunk(
      ClientResponse<FullResponseHolder> response,
      HttpChunk chunk
  )
  {
    BytesFullResponseHolder holder = (BytesFullResponseHolder) response.getObj();

    if (holder == null) {
      return ClientResponse.finished(null);
    }

    holder.addChunk(getContentBytes(chunk.getContent()));
    return response;
  }

  @Override
  public ClientResponse<FullResponseHolder> done(ClientResponse<FullResponseHolder> response)
  {
    return ClientResponse.finished(response.getObj());
  }

  @Override
  public void exceptionCaught(
      ClientResponse<FullResponseHolder> clientResponse, Throwable e
  )
  {
    // Its safe to Ignore as the ClientResponse returned in handleChunk were unfinished
  }

  private byte[] getContentBytes(ChannelBuffer content)
  {
    byte[] contentBytes = new byte[content.readableBytes()];
    content.readBytes(contentBytes);
    return contentBytes;
  }
}
