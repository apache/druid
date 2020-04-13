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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * {@link HttpResponseHandler} for stream data of byte array type.
 * The stream data is appended to {@link BytesFullResponseHolder} whenever {@link #handleChunk} is called.
 */
public class BytesFullResponseHandler implements HttpResponseHandler<BytesFullResponseHolder, BytesFullResponseHolder>
{
  @Override
  public ClientResponse<BytesFullResponseHolder> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    BytesFullResponseHolder holder = new BytesFullResponseHolder(response.getStatus(), response);

    holder.addChunk(getContentBytes(response.getContent()));

    return ClientResponse.unfinished(
        holder
    );
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> handleChunk(
      ClientResponse<BytesFullResponseHolder> response,
      HttpChunk chunk,
      long chunkNum
  )
  {
    BytesFullResponseHolder holder = response.getObj();

    if (holder == null) {
      return ClientResponse.finished(null);
    }

    holder.addChunk(getContentBytes(chunk.getContent()));
    return response;
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> done(ClientResponse<BytesFullResponseHolder> response)
  {
    return ClientResponse.finished(response.getObj());
  }

  @Override
  public void exceptionCaught(ClientResponse<BytesFullResponseHolder> clientResponse, Throwable e)
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
