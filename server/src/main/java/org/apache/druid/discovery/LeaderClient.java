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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;

/**
 * This is used to facilitate interaction with Coordinator/Overlord leader nodes. Instance of this interface is injected
 * via Guice with annotations @Coordinator or @IndexingService .
 */
public interface LeaderClient
{

  void start();

  void stop();

  /**
   * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException;

  /**
   * Executes a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  FullResponseHolder go(Request request, HttpResponseHandler<FullResponseHolder, FullResponseHolder> responseHandler)
      throws IOException, InterruptedException;

  /**
   * Executes the request object aimed at the leader and process the response with given handler
   */
  <Intermediate, Final> ListenableFuture<Final> goAsync(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler
  );

}
