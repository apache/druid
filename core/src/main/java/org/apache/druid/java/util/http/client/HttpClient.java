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

package org.apache.druid.java.util.http.client;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

/**
 * Interface for Async HTTP client libraries.
 */
public interface HttpClient
{
  /**
   * Submit a request and process the response with the given response handler.
   * <p>
   * Note that the Request object passed in to the HttpClient *may* be mutated by the actual client.  This is
   * largely done by composed clients, but the contract is that mutation is possible.  It is the caller's
   * responsibility to pass in a copy of the Request object if they want to have an object that is not mutated.
   *
   * @param request        Request to process, this *may* be mutated by the client
   * @param handler        An asynchronous response handler that will be used to process results from the http call
   * @param <Intermediate> The type of the intermediate results from the handler
   * @param <Final>        The type of the final results that the returned ListenableFuture will contain
   *
   * @return A listenable future that will eventually provide an object of type Final
   */
  <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler
  );

  /**
   * Submit a request and process the response with the given response handler.
   * <p>
   * Note that the Request object passed in to the HttpClient *may* be mutated by the actual client.  This is
   * largely done by composed clients, but the contract is that mutation is possible.  It is the caller's
   * responsibility to pass in a copy of the Request object if they want to have an object that is not mutated.
   *
   * @param request        Request to process, this *may* be mutated by the client
   * @param handler        An asynchronous response handler that will be used to process results from the http call
   * @param readTimeout    Read timeout to use for this request. Leave null to use the default readTimeout. Set to zero
   *                       to disable timeouts for this request.
   * @param <Intermediate> The type of the intermediate results from the handler
   * @param <Final>        The type of the final results that the returned ListenableFuture will contain
   *
   * @return A listenable future that will eventually provide an object of type Final
   */
  <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration readTimeout
  );
}
