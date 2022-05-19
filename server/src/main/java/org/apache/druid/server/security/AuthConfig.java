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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
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

  public static final String DRUID_ALLOW_UNSECURED_PATH = "Druid-Allow-Unsecured-Path";

  public static final String ALLOW_ALL_NAME = "allowAll";

  public static final String ANONYMOUS_NAME = "anonymous";

  public static final String TRUSTED_DOMAIN_NAME = "trustedDomain";

  public AuthConfig()
  {
    this(null, null, null, false, false);
  }

  @JsonCreator
  public AuthConfig(
      @JsonProperty("authenticatorChain") List<String> authenticatorChain,
      @JsonProperty("authorizers") List<String> authorizers,
      @JsonProperty("unsecuredPaths") List<String> unsecuredPaths,
      @JsonProperty("allowUnauthenticatedHttpOptions") boolean allowUnauthenticatedHttpOptions,
      @JsonProperty("authorizeQueryContextParams") boolean authorizeQueryContextParams
  )
  {
    this.authenticatorChain = authenticatorChain;
    this.authorizers = authorizers;
    this.unsecuredPaths = unsecuredPaths == null ? Collections.emptyList() : unsecuredPaths;
    this.allowUnauthenticatedHttpOptions = allowUnauthenticatedHttpOptions;
    this.authorizeQueryContextParams = authorizeQueryContextParams;
  }

  @JsonProperty
  private final List<String> authenticatorChain;

  @JsonProperty
  private final List<String> authorizers;

  @JsonProperty
  private final List<String> unsecuredPaths;

  @JsonProperty
  private final boolean allowUnauthenticatedHttpOptions;

  @JsonProperty
  private final boolean authorizeQueryContextParams;

  public List<String> getAuthenticatorChain()
  {
    return authenticatorChain;
  }

  public List<String> getAuthorizers()
  {
    return authorizers;
  }

  public List<String> getUnsecuredPaths()
  {
    return unsecuredPaths;
  }

  public boolean isAllowUnauthenticatedHttpOptions()
  {
    return allowUnauthenticatedHttpOptions;
  }

  public boolean authorizeQueryContextParams()
  {
    return authorizeQueryContextParams;
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
    return allowUnauthenticatedHttpOptions == that.allowUnauthenticatedHttpOptions
           && authorizeQueryContextParams == that.authorizeQueryContextParams
           && Objects.equals(authenticatorChain, that.authenticatorChain)
           && Objects.equals(authorizers, that.authorizers)
           && Objects.equals(unsecuredPaths, that.unsecuredPaths);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        authenticatorChain,
        authorizers,
        unsecuredPaths,
        allowUnauthenticatedHttpOptions,
        authorizeQueryContextParams
    );
  }

  @Override
  public String toString()
  {
    return "AuthConfig{" +
           "authenticatorChain=" + authenticatorChain +
           ", authorizers=" + authorizers +
           ", unsecuredPaths=" + unsecuredPaths +
           ", allowUnauthenticatedHttpOptions=" + allowUnauthenticatedHttpOptions +
           ", enableQueryContextAuthorization=" + authorizeQueryContextParams +
           '}';
  }

  public static Builder newBuilder()
  {
    return new Builder();
  }

  /**
   * AuthConfig object is created via Jackson in production. This builder is for easier code maintenance in unit tests.
   */
  public static class Builder
  {
    private List<String> authenticatorChain;
    private List<String> authorizers;
    private List<String> unsecuredPaths;
    private boolean allowUnauthenticatedHttpOptions;
    private boolean authorizeQueryContextParams;

    public Builder setAuthenticatorChain(List<String> authenticatorChain)
    {
      this.authenticatorChain = authenticatorChain;
      return this;
    }

    public Builder setAuthorizers(List<String> authorizers)
    {
      this.authorizers = authorizers;
      return this;
    }

    public Builder setUnsecuredPaths(List<String> unsecuredPaths)
    {
      this.unsecuredPaths = unsecuredPaths;
      return this;
    }

    public Builder setAllowUnauthenticatedHttpOptions(boolean allowUnauthenticatedHttpOptions)
    {
      this.allowUnauthenticatedHttpOptions = allowUnauthenticatedHttpOptions;
      return this;
    }

    public Builder setAuthorizeQueryContextParams(boolean authorizeQueryContextParams)
    {
      this.authorizeQueryContextParams = authorizeQueryContextParams;
      return this;
    }

    public AuthConfig build()
    {
      return new AuthConfig(
          authenticatorChain,
          authorizers,
          unsecuredPaths,
          allowUnauthenticatedHttpOptions,
          authorizeQueryContextParams
      );
    }
  }
}
