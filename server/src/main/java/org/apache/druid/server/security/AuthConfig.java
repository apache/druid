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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

  /**
   * Set of context keys which are always permissible because something in the Druid
   * code itself sets the key before the security check.
   */
  public static final Set<String> ALLOWED_CONTEXT_KEYS = ImmutableSet.of(
      // Set in the Avatica server path
      QueryContexts.CTX_SQL_STRINGIFY_ARRAYS,
      // Set by the Router
      QueryContexts.CTX_SQL_QUERY_ID
  );

  public AuthConfig()
  {
    this(null, null, null, false, false, null, null);
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

  /**
   * The set of query context keys that are allowed, even when security is
   * enabled. A null value is the same as an empty set.
   */
  @JsonProperty
  private final Set<String> unsecuredContextKeys;

  /**
   * The set of query context keys to secure, when context security is
   * enabled. Null has a special meaning: it means to ignore this set.
   * Else, only the keys in this set are subject to security. If set,
   * the unsecured list is ignored.
   */
  @JsonProperty
  private final Set<String> securedContextKeys;

  @JsonCreator
  public AuthConfig(
      @JsonProperty("authenticatorChain") List<String> authenticatorChain,
      @JsonProperty("authorizers") List<String> authorizers,
      @JsonProperty("unsecuredPaths") List<String> unsecuredPaths,
      @JsonProperty("allowUnauthenticatedHttpOptions") boolean allowUnauthenticatedHttpOptions,
      @JsonProperty("authorizeQueryContextParams") boolean authorizeQueryContextParams,
      @JsonProperty("unsecuredContextKeys") Set<String> unsecuredContextKeys,
      @JsonProperty("securedContextKeys") Set<String> securedContextKeys
  )
  {
    this.authenticatorChain = authenticatorChain;
    this.authorizers = authorizers;
    this.unsecuredPaths = unsecuredPaths == null ? Collections.emptyList() : unsecuredPaths;
    this.allowUnauthenticatedHttpOptions = allowUnauthenticatedHttpOptions;
    this.authorizeQueryContextParams = authorizeQueryContextParams;
    this.unsecuredContextKeys = unsecuredContextKeys == null
        ? Collections.emptySet()
        : unsecuredContextKeys;
    this.securedContextKeys = securedContextKeys;
  }

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

  /**
   * Filter the user-supplied context keys based on the context key security
   * rules. If context key security is disabled, then allow all keys. Else,
   * apply the three key lists defined here.
   * <ul>
   * <li>Allow Druid-defined keys.</li>
   * <li>Allow anything not in the secured context key list.</li>
   * <li>Allow anything in the config-defined unsecured key list.</li>
   * </ul>
   * In the typical case, a site defines either the secured key list
   * (to handle a few keys that are <i>are not</i> allowed) or the unsecured key
   * list (to enumerate a few that <i>are</i> allowed.) If both lists
   * are given, think of the secured list as exceptions to the unsecured
   * key list.
   *
   * @return the list of secured keys to check via authentication
   */
  public Set<String> contextKeysToAuthorize(final Set<String> userKeys)
  {
    if (!authorizeQueryContextParams) {
      return ImmutableSet.of();
    }
    Set<String> keysToCheck = CollectionUtils.subtract(userKeys, ALLOWED_CONTEXT_KEYS);
    keysToCheck = CollectionUtils.subtract(keysToCheck, unsecuredContextKeys);
    if (securedContextKeys != null) {
      keysToCheck = CollectionUtils.intersect(keysToCheck, securedContextKeys);
    }
    return keysToCheck;
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
           && Objects.equals(unsecuredPaths, that.unsecuredPaths)
           && Objects.equals(unsecuredContextKeys, that.unsecuredContextKeys)
           && Objects.equals(securedContextKeys, that.securedContextKeys);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        authenticatorChain,
        authorizers,
        unsecuredPaths,
        allowUnauthenticatedHttpOptions,
        authorizeQueryContextParams,
        unsecuredContextKeys,
        securedContextKeys
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
           ", unsecuredContextKeys=" + unsecuredContextKeys +
           ", securedContextKeys=" + securedContextKeys +
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
    private Set<String> unsecuredContextKeys;
    private Set<String> securedContextKeys;

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

    public Builder setUnsecuredContextKeys(Set<String> unsecuredContextKeys)
    {
      this.unsecuredContextKeys = unsecuredContextKeys;
      return this;
    }

    public Builder setSecuredContextKeys(Set<String> securedContextKeys)
    {
      this.securedContextKeys = securedContextKeys;
      return this;
    }

    public AuthConfig build()
    {
      return new AuthConfig(
          authenticatorChain,
          authorizers,
          unsecuredPaths,
          allowUnauthenticatedHttpOptions,
          authorizeQueryContextParams,
          unsecuredContextKeys,
          securedContextKeys
      );
    }
  }
}
