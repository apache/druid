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

package org.apache.druid.rpc;

import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * An HTTP response handler that discards the response and returns nothing. It returns a finished response only
 * when the entire HTTP response is done.
 */
public class IgnoreHttpResponseHandler implements HttpResponseHandler<Void, Void>
{
  public static final IgnoreHttpResponseHandler INSTANCE = new IgnoreHttpResponseHandler();

  private IgnoreHttpResponseHandler()
  {
    // Singleton.
  }

  @Override
  public ClientResponse<Void> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    return ClientResponse.unfinished(null);
  }

  @Override
  public ClientResponse<Void> handleChunk(ClientResponse<Void> clientResponse, HttpChunk chunk, long chunkNum)
  {
    return ClientResponse.unfinished(null);
  }

  @Override
  public ClientResponse<Void> done(ClientResponse<Void> clientResponse)
  {
    return ClientResponse.finished(null);
  }

  @Override
  public void exceptionCaught(ClientResponse<Void> clientResponse, Throwable e)
  {
    // Safe to ignore, since the ClientResponses returned in handleChunk were unfinished.
  }
}
