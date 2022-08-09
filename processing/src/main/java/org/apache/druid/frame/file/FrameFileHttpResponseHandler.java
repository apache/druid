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

package org.apache.druid.frame.file;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * An {@link HttpResponseHandler} that streams data into a {@link ReadableByteChunksFrameChannel}.
 *
 * It is not required that the request actually fetches an entire frame file. The return object for this handler,
 * {@link FrameFilePartialFetch}, allows callers to tell how much of the file was read in a particular request.
 *
 * This handler implements backpressure through {@link FrameFilePartialFetch#backpressureFuture()}. This allows callers
 * to back off from issuing the next request, if appropriate. However: the handler does not implement backpressure
 * through the {@link HttpResponseHandler.TrafficCop} mechanism. Therefore, it is important that each request retrieve
 * a modest amount of data.
 */
public class FrameFileHttpResponseHandler implements HttpResponseHandler<FrameFilePartialFetch, FrameFilePartialFetch>
{
  private final ReadableByteChunksFrameChannel channel;

  public FrameFileHttpResponseHandler(final ReadableByteChunksFrameChannel channel)
  {
    this.channel = Preconditions.checkNotNull(channel, "channel");
  }

  @Override
  public ClientResponse<FrameFilePartialFetch> handleResponse(final HttpResponse response, final TrafficCop trafficCop)
  {
    final ClientResponse<FrameFilePartialFetch> clientResponse = ClientResponse.unfinished(new FrameFilePartialFetch());

    if (response.getStatus().getCode() != HttpResponseStatus.OK.getCode()) {
      // Note: if the error body is chunked, we will discard all future chunks due to setting exceptionCaught here.
      // This is OK because we don't need the body; just the HTTP status code.
      exceptionCaught(clientResponse, new ISE("Server for [%s] returned [%s]", channel.getId(), response.getStatus()));
      return clientResponse;
    } else {
      return response(clientResponse, response.getContent());
    }
  }

  @Override
  public ClientResponse<FrameFilePartialFetch> handleChunk(
      final ClientResponse<FrameFilePartialFetch> clientResponse,
      final HttpChunk chunk,
      final long chunkNum
  )
  {
    return response(clientResponse, chunk.getContent());
  }

  @Override
  public ClientResponse<FrameFilePartialFetch> done(final ClientResponse<FrameFilePartialFetch> clientResponse)
  {
    return ClientResponse.finished(clientResponse.getObj());
  }

  @Override
  public void exceptionCaught(
      final ClientResponse<FrameFilePartialFetch> clientResponse,
      final Throwable e
  )
  {
    clientResponse.getObj().exceptionCaught(e);
  }

  private ClientResponse<FrameFilePartialFetch> response(
      final ClientResponse<FrameFilePartialFetch> clientResponse,
      final ChannelBuffer content
  )
  {
    final FrameFilePartialFetch clientResponseObj = clientResponse.getObj();

    if (clientResponseObj.isExceptionCaught()) {
      // If there was an exception, exit early without updating "channel". Important because "handleChunk" can be
      // called after "handleException" in two cases: it can be called after an error "response", if the error body
      // is chunked; and it can be called when "handleChunk" is called after "exceptionCaught". In neither case do
      // we want to add that extra chunk to the channel.
      return ClientResponse.finished(clientResponseObj);
    }

    final byte[] chunk = new byte[content.readableBytes()];
    content.getBytes(content.readerIndex(), chunk);

    try {
      final ListenableFuture<?> backpressureFuture = channel.addChunk(chunk);

      if (backpressureFuture != null) {
        clientResponseObj.setBackpressureFuture(backpressureFuture);
      }

      clientResponseObj.addBytesRead(chunk.length);
    }
    catch (Exception e) {
      clientResponseObj.exceptionCaught(e);
    }

    return ClientResponse.unfinished(clientResponseObj);
  }
}
