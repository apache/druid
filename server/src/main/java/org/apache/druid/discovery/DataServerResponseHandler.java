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

package org.apache.druid.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResource;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;

public class DataServerResponseHandler implements HttpResponseHandler<AppendableByteArrayInputStream, InputStream>
{
  private final String queryId;
  private final ResponseContext responseContext;
  private final ObjectMapper objectMapper;

  public <T> DataServerResponseHandler(Query<T> query, ResponseContext responseContext, ObjectMapper objectMapper)
  {
    this.queryId = query.getId();
    this.responseContext = responseContext;
    this.objectMapper = objectMapper;
  }

  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    in.add(getContentBytes(response.getContent()));

    try {
      final String queryResponseHeaders = response.headers().get(QueryResource.HEADER_RESPONSE_CONTEXT);
      if (queryResponseHeaders != null) {
        responseContext.merge(ResponseContext.deserialize(queryResponseHeaders, objectMapper));
      }
      return ClientResponse.finished(in);
    }
    catch (IOException e) {
      return ClientResponse.finished(
          new AppendableByteArrayInputStream()
          {
            @Override
            public int read() throws IOException
            {
              throw e;
            }
          }
      );
    }
  }

  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleChunk(
      ClientResponse<AppendableByteArrayInputStream> clientResponse,
      HttpChunk chunk,
      long chunkNum
  )
  {
    clientResponse.getObj().add(getContentBytes(chunk.getContent()));
    return clientResponse;
  }

  @Override
  public ClientResponse<InputStream> done(ClientResponse<AppendableByteArrayInputStream> clientResponse)
  {
    final AppendableByteArrayInputStream obj = clientResponse.getObj();
    obj.done();
    return ClientResponse.finished(obj);
  }

  @Override
  public void exceptionCaught(ClientResponse<AppendableByteArrayInputStream> clientResponse, Throwable e)
  {
    final AppendableByteArrayInputStream obj = clientResponse.getObj();
    obj.exceptionCaught(e);
  }

  private byte[] getContentBytes(ChannelBuffer content)
  {
    byte[] contentBytes = new byte[content.readableBytes()];
    content.readBytes(contentBytes);
    return contentBytes;
  }
}
