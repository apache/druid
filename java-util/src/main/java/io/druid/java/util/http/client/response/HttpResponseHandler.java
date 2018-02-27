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
 */
public interface HttpResponseHandler<IntermediateType, FinalType>
{
  /**
   * Handles the initial HttpResponse object that comes back from Netty.
   *
   * @param response - response from Netty
   * @return
   */
  ClientResponse<IntermediateType> handleResponse(HttpResponse response);
  ClientResponse<IntermediateType> handleChunk(ClientResponse<IntermediateType> clientResponse, HttpChunk chunk);
  ClientResponse<FinalType> done(ClientResponse<IntermediateType> clientResponse);
  void exceptionCaught(ClientResponse<IntermediateType> clientResponse, Throwable e);
}
