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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OIDCConfigTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();

    String jsonStr = "{\n"
                     + "  \"clientID\": \"testid\",\n"
                     + "  \"clientSecret\": \"testsecret\",\n"
                     + "  \"discoveryURI\": \"testdiscoveryuri\",\n"
                     + "  \"scope\": \"testscope\"\n"
                     + "}\n";

    OIDCConfig conf = jsonMapper.readValue(
        jsonMapper.writeValueAsString(jsonMapper.readValue(jsonStr, OIDCConfig.class)),
        OIDCConfig.class
    );
    Assertions.assertEquals("testid", conf.getClientID());
    Assertions.assertEquals("testsecret", conf.getClientSecret().getPassword());
    Assertions.assertEquals("testdiscoveryuri", conf.getDiscoveryURI());
    Assertions.assertEquals("name", conf.getOidcClaim());
    Assertions.assertEquals("testscope", conf.getScope());
    Assertions.assertNull(conf.getClientAuthenticationMethod());
  }

  @Test
  public void testSerdeWithoutDefaults() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();

    String jsonStr = "{\n"
                     + "  \"clientID\": \"testid\",\n"
                     + "  \"clientSecret\": \"testsecret\",\n"
                     + "  \"discoveryURI\": \"testdiscoveryuri\",\n"
                     + "  \"oidcClaim\": \"email\",\n"
                     + "  \"scope\": \"testscope\"\n"
                     + "}\n";

    OIDCConfig conf = jsonMapper.readValue(
        jsonMapper.writeValueAsString(jsonMapper.readValue(jsonStr, OIDCConfig.class)),
        OIDCConfig.class
    );

    Assertions.assertEquals("testid", conf.getClientID());
    Assertions.assertEquals("testsecret", conf.getClientSecret().getPassword());
    Assertions.assertEquals("testdiscoveryuri", conf.getDiscoveryURI());
    Assertions.assertEquals("email", conf.getOidcClaim());
    Assertions.assertEquals("testscope", conf.getScope());
  }

  @Test
  public void testSerdeWithClientAuthenticationMethod() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();

    String jsonStr = "{\n"
                     + "  \"clientID\": \"testid\",\n"
                     + "  \"clientSecret\": \"testsecret\",\n"
                     + "  \"discoveryURI\": \"testdiscoveryuri\",\n"
                     + "  \"oidcClaim\": \"email\",\n"
                     + "  \"scope\": \"testscope\",\n"
                     + "  \"clientAuthenticationMethod\": \"client_secret_post\"\n"
                     + "}\n";

    OIDCConfig conf = jsonMapper.readValue(
        jsonMapper.writeValueAsString(jsonMapper.readValue(jsonStr, OIDCConfig.class)),
        OIDCConfig.class
    );

    Assertions.assertEquals("testid", conf.getClientID());
    Assertions.assertEquals("testsecret", conf.getClientSecret().getPassword());
    Assertions.assertEquals("testdiscoveryuri", conf.getDiscoveryURI());
    Assertions.assertEquals("email", conf.getOidcClaim());
    Assertions.assertEquals("testscope", conf.getScope());
    Assertions.assertEquals("client_secret_post", conf.getClientAuthenticationMethod());
  }

  @Test
  public void testUnsupportedClientAuthenticationMethods()
  {
    for (String method : new String[]{"client_secret_jwt", "private_key_jwt", "bogus"}) {
      DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> new OIDCConfig("testid", new DefaultPasswordProvider("testsecret"), "testdiscoveryuri", null, null, method)
      );
      Assertions.assertEquals(
          "druid.auth.pac4j.oidc.clientAuthenticationMethod[" + method + "] is not supported. "
          + "Supported values are [client_secret_basic, client_secret_post, none], "
          + "or leave it unset to pick one from the provider's discovery document.",
          e.getMessage()
      );
    }
  }

  @Test
  public void testClientSecretRequired()
  {
    for (String method : new String[]{null, "client_secret_basic", "client_secret_post"}) {
      DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> new OIDCConfig("testid", null, "testdiscoveryuri", null, null, method)
      );
      Assertions.assertEquals(
          "druid.auth.pac4j.oidc.clientSecret is required unless "
          + "druid.auth.pac4j.oidc.clientAuthenticationMethod is [none].",
          e.getMessage()
      );
    }
  }

  @Test
  public void testNoneWithoutClientSecret()
  {
    OIDCConfig conf = new OIDCConfig("testid", null, "testdiscoveryuri", null, null, "none");
    Assertions.assertNull(conf.getClientSecret());
    Assertions.assertEquals("none", conf.getClientAuthenticationMethod());
  }

  @Test
  public void testNoneWithClientSecret()
  {
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> new OIDCConfig("testid", new DefaultPasswordProvider("testsecret"), "testdiscoveryuri", null, null, "none")
    );
    Assertions.assertEquals(
        "druid.auth.pac4j.oidc.clientSecret must not be set when "
        + "druid.auth.pac4j.oidc.clientAuthenticationMethod is [none].",
        e.getMessage()
    );
  }
}
