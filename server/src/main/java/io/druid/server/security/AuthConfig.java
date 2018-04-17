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

package io.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class AuthConfig
{
  /**
   * HTTP attribute that holds an AuthenticationResult, with info about a successful authentication check.
   */
  public static final String DRUID_AUTHENTICATION_RESULT = "Druid-Authentication-Result";

  /**
   * HTTP attribute set when a static method in AuthorizationUtils performs an authorization check on the request.
   */
  public static final String DRUID_AUTHORIZATION_CHECKED = "Druid-Authorization-Checked";

  public static final String ALLOW_ALL_NAME = "allowAll";

  public AuthConfig()
  {
    this(null, null, false);
  }

  @JsonCreator
  public AuthConfig(
      @JsonProperty("authenticatorChain") List<String> authenticationChain,
      @JsonProperty("authorizers") List<String> authorizers,
      @JsonProperty("allowUnauthenticatedHttpOptions") boolean allowUnauthenticatedHttpOptions
  )
  {
    this.authenticatorChain = authenticationChain;
    this.authorizers = authorizers;
    this.allowUnauthenticatedHttpOptions = allowUnauthenticatedHttpOptions;
  }

  @JsonProperty
  private final List<String> authenticatorChain;

  @JsonProperty
  private List<String> authorizers;

  @JsonProperty
  private final boolean allowUnauthenticatedHttpOptions;

  public List<String> getAuthenticatorChain()
  {
    return authenticatorChain;
  }

  public List<String> getAuthorizers()
  {
    return authorizers;
  }

  public boolean isAllowUnauthenticatedHttpOptions()
  {
    return allowUnauthenticatedHttpOptions;
  }

  @Override
  public String toString()
  {
    return "AuthConfig{" +
           "authenticatorChain=" + authenticatorChain +
           ", authorizers=" + authorizers +
           ", allowUnauthenticatedHttpOptions=" + allowUnauthenticatedHttpOptions +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthConfig that = (AuthConfig) o;
    return isAllowUnauthenticatedHttpOptions() == that.isAllowUnauthenticatedHttpOptions() &&
           Objects.equals(getAuthenticatorChain(), that.getAuthenticatorChain()) &&
           Objects.equals(getAuthorizers(), that.getAuthorizers());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getAuthenticatorChain(), getAuthorizers(), isAllowUnauthenticatedHttpOptions());
  }
}
