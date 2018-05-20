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

package io.druid.security.basic.authentication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.java.util.http.client.CredentialedHttpClient;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.auth.BasicCredentials;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Escalator;

@JsonTypeName("basic")
public class BasicHTTPEscalator implements Escalator
{
  private final String internalClientUsername;
  private final String internalClientPassword;
  private final String authorizerName;

  @JsonCreator
  public BasicHTTPEscalator(
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("internalClientUsername") String internalClientUsername,
      @JsonProperty("internalClientPassword") String internalClientPassword
  )
  {
    this.authorizerName = authorizerName;
    this.internalClientUsername = internalClientUsername;
    this.internalClientPassword = internalClientPassword;
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return new CredentialedHttpClient(
        new BasicCredentials(internalClientUsername, internalClientPassword),
        baseClient
    );
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    // if you found your self asking why the authenticatedBy field is set to null please read this:
    // https://github.com/druid-io/druid/pull/5706#discussion_r185940889
    return new AuthenticationResult(internalClientUsername, authorizerName, null, null);
  }
}
