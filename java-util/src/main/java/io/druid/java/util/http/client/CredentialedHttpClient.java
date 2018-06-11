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

package io.druid.java.util.http.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.http.client.auth.Credentials;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

/**
 */
public class CredentialedHttpClient extends AbstractHttpClient
{

  private final Credentials creds;
  private final HttpClient delegate;

  public CredentialedHttpClient(Credentials creds, HttpClient delegate)
  {
    this.creds = creds;
    this.delegate = delegate;
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration requestReadTimeout
  )
  {
    return delegate.go(creds.addCredentials(request), handler, requestReadTimeout);
  }
}
