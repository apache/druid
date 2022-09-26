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

import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * This class is to hold data while receiving stream data via HTTP. Used with {@link HttpResponseHandler}.
 *
 * @param <T> data type
 */
public abstract class FullResponseHolder<T>
{
  private final HttpResponse response;

  public FullResponseHolder(HttpResponse response)
  {
    this.response = response;
  }

  public HttpResponseStatus getStatus()
  {
    return response.status();
  }

  public HttpResponse getResponse()
  {
    return response;
  }

  /**
   * Get the data.
   */
  public abstract T getContent();
}
