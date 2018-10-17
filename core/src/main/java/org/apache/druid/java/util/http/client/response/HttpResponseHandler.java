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

/**
 * A handler for an HTTP request.
 *
 * The ClientResponse object passed around is used to store state between further chunks and indicate when it is safe
 * to hand the object back to the caller.
 *
 * If the response is chunked, the ClientResponse object returned from handleResponse will be passed in as the
 * first argument to handleChunk().
 *
 * If the ClientResponse object is marked as finished, that indicates that the object stored is safe to hand
 * off to the caller.  This is most often done either from the done() method after all content has been processed or
 * from the initial handleResponse method to indicate that the object is thread-safe and aware that it might be
 * accessed before all chunks come back.
 *
 * Note: if you return a finished ClientResponse object from anything other than the done() method, IntermediateType
 * must be castable to FinalType
 *
 * This handler can exert backpressure by returning a response with "continueReading" set to false from handleResponse()
 * or handleChunk(). In this case, the HTTP client will stop reading soon thereafter. It may not happen immediately, so
 * be prepared for more handleChunk() calls to happen. To resume reads, call resume() on the TrafficCop provided by
 * handleResponse() with a chunk number at least as high as the one provided by the handleChunk() call from which you
 * returned a suspend-reading response. If you are resuming reads after suspending them from handleResponse(), use 0
 * for the chunk number.
 */
public interface HttpResponseHandler<IntermediateType, FinalType>
{
  /**
   * Handles the initial HttpResponse object that comes back from Netty.
   *
   * @param response   response from Netty
   * @param trafficCop flow controller, allows resuming suspended reads
   *
   * @return response that may be "finished" or "unfinished".
   */
  ClientResponse<IntermediateType> handleResponse(HttpResponse response, TrafficCop trafficCop);

  /**
   * Called for chunked responses, indicating another HttpChunk has arrived.
   *
   * @param clientResponse last response returned by the prior handleResponse() or handleChunk()
   * @param chunk          the new chunk of data
   * @param chunkNum       the sequence number of this chunk (increases monotonically)
   *
   * @return response that may be "finished" or "unfinished".
   */
  ClientResponse<IntermediateType> handleChunk(
      ClientResponse<IntermediateType> clientResponse,
      HttpChunk chunk,
      long chunkNum
  );

  /**
   * Called after the final handleResponse() or handleChunk() call, signifying that no more data
   * will arrive.
   *
   * @param clientResponse last response returned by handleResponse() or handleChunk()
   *
   * @return response containing an object to hand back to the caller. It must be a "finished" response.
   */
  ClientResponse<FinalType> done(ClientResponse<IntermediateType> clientResponse);

  void exceptionCaught(ClientResponse<IntermediateType> clientResponse, Throwable e);

  interface TrafficCop
  {
    /**
     * Call this to resume reading after you have suspended it.
     *
     * @param chunkNum chunk number corresponding to the handleChunk() or handleResponse() call from which you
     * @return time that backpressure was applied (channel was closed for reads)
     */
    long resume(long chunkNum);
  }
}
