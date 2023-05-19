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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.profile.creator.TokenValidator;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("jwt")
public class JwtAuthenticator implements Authenticator
{
  private final String authorizerName;
  private final OIDCConfig oidcConfig;
  private final Supplier<TokenValidator> tokenValidatorSupplier;
  private final String name;

  @JsonCreator
  public JwtAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JacksonInject OIDCConfig oidcConfig
  )
  {
    this.name = name;
    this.oidcConfig = oidcConfig;
    this.authorizerName = authorizerName;

    this.tokenValidatorSupplier = Suppliers.memoize(() -> createTokenValidator(oidcConfig));
  }

  @Override
  public Filter getFilter()
  {
    return new JwtAuthFilter(authorizerName, name, oidcConfig, tokenValidatorSupplier.get());
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return JwtAuthFilter.class;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Nullable
  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Nullable
  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Nullable
  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    return null;
  }

  private TokenValidator createTokenValidator(OIDCConfig config)
  {
    OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(config.getClientID());
    oidcConfiguration.setSecret(config.getClientSecret().getPassword());
    oidcConfiguration.setDiscoveryURI(config.getDiscoveryURI());
    return new TokenValidator(oidcConfiguration);
  }
}
