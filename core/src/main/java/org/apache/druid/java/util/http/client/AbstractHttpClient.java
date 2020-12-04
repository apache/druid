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

public abstract class AbstractHttpClient implements HttpClient
{
  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler
  )
  {
    return go(request, handler, (Duration) null);
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(Request request,
      HttpResponseHandler<Intermediate, Final> handler, java.time.Duration readTimeout)
  {
    final Duration d = (readTimeout == null) ? null : new Duration(readTimeout.toMillis());
    return go(request, handler, d);
  }

}
