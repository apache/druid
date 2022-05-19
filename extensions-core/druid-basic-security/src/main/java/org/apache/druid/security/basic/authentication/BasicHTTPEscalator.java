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

package org.apache.druid.security.basic.authentication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;

@JsonTypeName("basic")
public class BasicHTTPEscalator implements Escalator
{
  private static final Logger LOG = new Logger(BasicHTTPEscalator.class);

  private final String internalClientUsername;
  private final PasswordProvider internalClientPassword;
  private final String authorizerName;

  @JsonCreator
  public BasicHTTPEscalator(
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("internalClientUsername") String internalClientUsername,
      @JsonProperty("internalClientPassword") PasswordProvider internalClientPassword
  )
  {
    this.authorizerName = authorizerName;
    this.internalClientUsername = internalClientUsername;
    this.internalClientPassword = internalClientPassword;
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    LOG.debug("----------- Creating escalated client");
    return new CredentialedHttpClient(
        new BasicCredentials(internalClientUsername, internalClientPassword.getPassword()),
        baseClient
    );
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    LOG.debug("----------- Creating escalated authentication result. username: %s", this.internalClientUsername);
    // if you found your self asking why the authenticatedBy field is set to null please read this:
    // https://github.com/apache/druid/pull/5706#discussion_r185940889
    return new AuthenticationResult(internalClientUsername, authorizerName, null, null);
  }
}
