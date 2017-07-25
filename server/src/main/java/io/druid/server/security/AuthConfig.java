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

public class AuthConfig
{
  /**
   * Use this String as the attribute name for the request attribute to pass an authentication token
   * from the servlet filter to the jersey resource
   */
  public static final String DRUID_AUTH_TOKEN = "Druid-Auth-Token";

  /**
   * HTTP attribute set when a static method in AuthorizationUtils performs an authorization check on the request.
   */
  public static final String DRUID_AUTH_TOKEN_CHECKED = "Druid-Auth-Token-Checked";

  /**
   * HTTP attribute that indicates the namespace for a request. Set by Authenticator implementations when
   * they successfully authenticate a request. The AuthorizationManager with a matching namespace will be used to
   * authorize the request.
   */
  public static final String DRUID_AUTH_NAMESPACE = "Druid-Auth-Namespace";

  public AuthConfig()
  {
    this(false, null, null, null);
  }

  @JsonCreator
  public AuthConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("authenticatorChain") List<String> authenticationChain,
      @JsonProperty("internalAuthenticator") String internalAuthenticator,
      @JsonProperty("authorizationManagers") List<String> authorizationManagers
  )
  {
    this.enabled = enabled;
    this.authenticatorChain = authenticationChain;
    this.internalAuthenticator = internalAuthenticator;
    this.authorizationManagers = authorizationManagers;
  }

  @JsonProperty
  private final boolean enabled;

  @JsonProperty
  private final List<String> authenticatorChain;

  @JsonProperty
  private final String internalAuthenticator;

  @JsonProperty
  List<String> authorizationManagers;

  public boolean isEnabled()
  {
    return enabled;
  }

  public List<String> getAuthenticatorChain()
  {
    return authenticatorChain;
  }

  public String getInternalAuthenticator()
  {
    return internalAuthenticator;
  }

  public List<String> getAuthorizationManagers()
  {
    return authorizationManagers;
  }

  @Override
  public String toString()
  {
    return "AuthConfig{" +
           "enabled=" + enabled +
           ", authenticatorChain='" + authenticatorChain + '\'' +
           ", internalAuthenticator='" + internalAuthenticator + '\'' +
           ", authorizationManagers='" + authorizationManagers + '\'' +
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

    if (isEnabled() != that.isEnabled()) {
      return false;
    }
    if (getAuthenticatorChain() != null
        ? !getAuthenticatorChain().equals(that.getAuthenticatorChain())
        : that.getAuthenticatorChain() != null) {
      return false;
    }
    if (getInternalAuthenticator() != null
        ? !getInternalAuthenticator().equals(that.getInternalAuthenticator())
        : that.getInternalAuthenticator() != null) {
      return false;
    }
    return getAuthorizationManagers() != null
           ? getAuthorizationManagers().equals(that.getAuthorizationManagers())
           : that.getAuthorizationManagers() == null;

  }

  @Override
  public int hashCode()
  {
    int result = (isEnabled() ? 1 : 0);
    result = 31 * result + (getAuthenticatorChain() != null ? getAuthenticatorChain().hashCode() : 0);
    result = 31 * result + (getInternalAuthenticator() != null ? getInternalAuthenticator().hashCode() : 0);
    result = 31 * result + (getAuthorizationManagers() != null ? getAuthorizationManagers().hashCode() : 0);
    return result;
  }

}
