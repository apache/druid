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

package org.apache.druid.msq.indexing.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.serde.ClusterByStatisticsSnapshotSerde;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class SketchResponseHandler implements HttpResponseHandler<BytesFullResponseHolder, ClusterByStatisticsSnapshot>
{
  private final ObjectMapper jsonMapper;
  private Function<BytesFullResponseHolder, ClusterByStatisticsSnapshot> deserializerFunction;

  public SketchResponseHandler(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ClientResponse<BytesFullResponseHolder> handleResponse(HttpResponse response, HttpResponseHandler.TrafficCop
      trafficCop)
  {
    final BytesFullResponseHolder holder = new BytesFullResponseHolder(response);
    final String contentType = response.headers().get(HttpHeaders.CONTENT_TYPE);
    if (MediaType.APPLICATION_OCTET_STREAM.equals(contentType)) {
      deserializerFunction = responseHolder -> ClusterByStatisticsSnapshotSerde.deserialize(ByteBuffer.wrap(responseHolder.getContent()));
    } else {
      deserializerFunction = responseHolder -> responseHolder.deserialize(jsonMapper, new TypeReference<ClusterByStatisticsSnapshot>()
      {
      });
    }
    holder.addChunk(getContentBytes(response.getContent()));

    return ClientResponse.unfinished(holder);
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
  public ClientResponse<ClusterByStatisticsSnapshot> done(ClientResponse<BytesFullResponseHolder> response)
  {
    return ClientResponse.finished(deserializerFunction.apply(response.getObj()));
  }

  @Override
  public void exceptionCaught(ClientResponse<BytesFullResponseHolder> clientResponse, Throwable e)
  {
  }

  private byte[] getContentBytes(ChannelBuffer content)
  {
    byte[] contentBytes = new byte[content.readableBytes()];
    content.readBytes(contentBytes);
    return contentBytes;
  }
}
