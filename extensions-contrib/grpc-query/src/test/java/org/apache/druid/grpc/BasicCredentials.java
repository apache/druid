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

package org.apache.druid.grpc;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Executor;

/**
 * Applies Basic Auth credentials to an outgoing request. Use this
 * class to set the Basic Auth user name and password on a gRPC client:
 * <pre><code>
 * QueryBlockingStub client = QueryGrpc.newBlockingStub(channel)
 *     .withCallCredentials( new BasicCredentials(user, password));
 * </code></pre>
 */
public class BasicCredentials extends CallCredentials
{
  public static final String AUTHORIZATION_HEADER = "Authorization";
  private static final Metadata.Key<String> AUTH_KEY = Metadata.Key.of(AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER);

  public final String user;
  public final String password;

  public BasicCredentials(String user, String password)
  {
    this.user = user;
    this.password = password;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo, Executor exec, MetadataApplier applier)
  {
    Metadata metadata = new Metadata();
    metadata.put(AUTH_KEY, getBasicAuthenticationHeader(user, password));
    applier.apply(metadata);
  }

  // Source: https://www.baeldung.com/java-httpclient-basic-auth#authenticate-using-http-headers
  private static String getBasicAuthenticationHeader(String username, String password)
  {
    String valueToEncode = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void thisUsesUnstableApi()
  {
    // We've been warned.
  }
}
