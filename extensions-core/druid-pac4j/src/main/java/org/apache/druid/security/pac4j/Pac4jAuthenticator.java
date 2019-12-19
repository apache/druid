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
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.pac4j.core.config.Config;
import org.pac4j.core.http.callback.NoParameterCallbackUrlResolver;
import org.pac4j.core.http.url.DefaultUrlResolver;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("pac4j")
public class Pac4jAuthenticator implements Authenticator
{
  private final String name;
  private final String authorizerName;
  private final OIDCConfig oidcConfig;

  @JsonCreator
  public Pac4jAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JacksonInject OIDCConfig oidcConfig
  )
  {
    this.name = name;
    this.authorizerName = authorizerName;
    this.oidcConfig = oidcConfig;
  }

  @Override
  public Filter getFilter()
  {
    return new Pac4jFilter(
        name,
        authorizerName,
        createPac4jConfig(oidcConfig),
        oidcConfig.getCookiePassphrase().getPassword()
    );
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Override
  @Nullable
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    return null;
  }


  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
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

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  private Config createPac4jConfig(OIDCConfig oidcConfig)
  {
    OidcConfiguration oidcConf = new OidcConfiguration();
    oidcConf.setClientId(oidcConfig.getClientID());
    oidcConf.setSecret(oidcConfig.getClientSecret().getPassword());
    oidcConf.setDiscoveryURI(oidcConfig.getDiscoveryURI());
    oidcConf.setExpireSessionWithToken(true);
    oidcConf.setUseNonce(true);

    OidcClient oidcClient = new OidcClient(oidcConf);
    oidcClient.setUrlResolver(new DefaultUrlResolver(true));
    oidcClient.setCallbackUrlResolver(new NoParameterCallbackUrlResolver());

    return new Config(Pac4jCallbackResource.SELF_URL, oidcClient);
  }
}
