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

package org.apache.druid.security.pac4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import org.apache.druid.error.DruidException;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.util.Set;

public class OIDCConfig
{
  private static final String DEFAULT_SCOPE = "name";

  /**
   * Client authentication methods that work with the pac4j configuration Druid builds. pac4j does not implement
   * client_secret_jwt, and private_key_jwt needs a private key that Druid has no properties for.
   */
  private static final Set<String> SUPPORTED_CLIENT_AUTHENTICATION_METHODS = ImmutableSet.of(
      ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue(),
      ClientAuthenticationMethod.CLIENT_SECRET_POST.getValue(),
      ClientAuthenticationMethod.NONE.getValue()
  );

  @JsonProperty
  private final String clientID;

  @JsonProperty
  private final PasswordProvider clientSecret;

  @JsonProperty
  private final String discoveryURI;

  @JsonProperty
  private final String oidcClaim;

  @JsonProperty
  private final String scope;

  @JsonProperty
  private final String clientAuthenticationMethod;

  @JsonCreator
  public OIDCConfig(
      @JsonProperty("clientID") String clientID,
      @JsonProperty("clientSecret") @Nullable PasswordProvider clientSecret,
      @JsonProperty("discoveryURI") String discoveryURI,
      @JsonProperty("oidcClaim") String oidcClaim,
      @JsonProperty("scope") @Nullable String scope,
      @JsonProperty("clientAuthenticationMethod") @Nullable String clientAuthenticationMethod
  )
  {
    this.clientID = Preconditions.checkNotNull(clientID, "null clientID");
    if (clientAuthenticationMethod != null
        && !SUPPORTED_CLIENT_AUTHENTICATION_METHODS.contains(clientAuthenticationMethod)) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "druid.auth.pac4j.oidc.clientAuthenticationMethod[%s] is not supported. "
                              + "Supported values are %s, or leave it unset to pick one from the provider's discovery document.",
                              clientAuthenticationMethod,
                              SUPPORTED_CLIENT_AUTHENTICATION_METHODS
                          );
    }
    if (ClientAuthenticationMethod.NONE.getValue().equals(clientAuthenticationMethod)) {
      if (clientSecret != null) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "druid.auth.pac4j.oidc.clientSecret must not be set when "
                                + "druid.auth.pac4j.oidc.clientAuthenticationMethod is [none]."
                            );
      }
    } else if (clientSecret == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "druid.auth.pac4j.oidc.clientSecret is required unless "
                              + "druid.auth.pac4j.oidc.clientAuthenticationMethod is [none]."
                          );
    }
    this.clientSecret = clientSecret;
    this.discoveryURI = Preconditions.checkNotNull(discoveryURI, "null discoveryURI");
    this.oidcClaim = oidcClaim == null ? DEFAULT_SCOPE : oidcClaim;
    this.scope = scope;
    this.clientAuthenticationMethod = clientAuthenticationMethod;
  }

  @JsonProperty
  public String getClientID()
  {
    return clientID;
  }

  @JsonProperty
  @Nullable
  public PasswordProvider getClientSecret()
  {
    return clientSecret;
  }

  @JsonProperty
  public String getDiscoveryURI()
  {
    return discoveryURI;
  }

  @JsonProperty
  public String getOidcClaim()
  {
    return oidcClaim;
  }

  @JsonProperty
  public String getScope()
  {
    return scope;
  }

  @JsonProperty
  public String getClientAuthenticationMethod()
  {
    return clientAuthenticationMethod;
  }
}
