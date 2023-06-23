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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;

/**
 * This is a clone of {@link InputStreamResponseHandler} except that it retains HTTP status/response object in the
 * response holder result.
 */
public class InputStreamFullResponseHandler implements HttpResponseHandler<InputStreamFullResponseHolder, InputStreamFullResponseHolder>
{
  @Override
  public ClientResponse<InputStreamFullResponseHolder> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    InputStreamFullResponseHolder holder = new InputStreamFullResponseHolder(response);
    return ClientResponse.finished(holder);
  }

  @Override
  public ClientResponse<InputStreamFullResponseHolder> handleChunk(
      ClientResponse<InputStreamFullResponseHolder> clientResponse,
      HttpContent chunk,
      long chunkNum
  )
  {
    clientResponse.getObj().addChunk(getContentBytes(chunk.content()));
    return clientResponse;
  }

  @Override
  public ClientResponse<InputStreamFullResponseHolder> done(ClientResponse<InputStreamFullResponseHolder> clientResponse)
  {
    InputStreamFullResponseHolder holder = clientResponse.getObj();
    holder.done();
    return ClientResponse.finished(holder);
  }

  @Override
  public void exceptionCaught(
      ClientResponse<InputStreamFullResponseHolder> clientResponse,
      Throwable e
  )
  {
    clientResponse.getObj().exceptionCaught(e);
  }

  private byte[] getContentBytes(ByteBuf content)
  {
    byte[] contentBytes = new byte[content.readableBytes()];
    content.readBytes(contentBytes);
    return contentBytes;
  }
}
