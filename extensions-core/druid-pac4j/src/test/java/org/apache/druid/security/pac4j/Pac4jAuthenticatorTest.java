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

import com.google.common.collect.ImmutableSet;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;

public class Pac4jAuthenticatorTest
{
  @Test
  public void testAutoDetectedClientAuthenticationMethods()
  {
    OidcConfiguration conf = createOidcConfiguration(new OIDCConfig(
        "testid",
        new DefaultPasswordProvider("testsecret"),
        "testdiscoveryuri",
        null,
        null,
        null
    ));
    Assertions.assertEquals("testsecret", conf.getSecret());
    Assertions.assertNull(conf.getClientAuthenticationMethod());
    Assertions.assertEquals(
        ImmutableSet.of(ClientAuthenticationMethod.CLIENT_SECRET_BASIC, ClientAuthenticationMethod.CLIENT_SECRET_POST),
        conf.getSupportedClientAuthenticationMethods()
    );
  }

  @Test
  public void testConfiguredClientAuthenticationMethod()
  {
    OidcConfiguration conf = createOidcConfiguration(new OIDCConfig(
        "testid",
        new DefaultPasswordProvider("testsecret"),
        "testdiscoveryuri",
        null,
        null,
        "client_secret_post"
    ));
    Assertions.assertEquals("testsecret", conf.getSecret());
    Assertions.assertEquals(ClientAuthenticationMethod.CLIENT_SECRET_POST, conf.getClientAuthenticationMethod());
  }

  @Test
  public void testNoneHasNoSecret()
  {
    OidcConfiguration conf = createOidcConfiguration(new OIDCConfig(
        "testid",
        null,
        "testdiscoveryuri",
        null,
        null,
        "none"
    ));
    Assertions.assertEquals("testid", conf.getClientId());
    Assertions.assertNull(conf.getSecret());
  }

  private static OidcConfiguration createOidcConfiguration(OIDCConfig oidcConfig)
  {
    Pac4jAuthenticator authenticator = new Pac4jAuthenticator(
        "pac4j",
        "allowAll",
        new Pac4jCommonConfig(false, new DefaultPasswordProvider("testpassphrase"), Duration.millis(1000)),
        oidcConfig,
        null
    );
    OidcClient client = (OidcClient) authenticator.getPac4jConfig().getClients().getClients().get(0);
    return client.getConfiguration();
  }
}
