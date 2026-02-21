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

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import org.junit.Test;
import org.pac4j.oidc.profile.OidcProfile;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoleBasedAuthGenTest
{

  @Test
  public void extractsRolesFromNestedArray()
  {
    Map<String, Object> druidms = new HashMap<>();
    druidms.put("roles", Arrays.asList("admin", "user", ""));

    Map<String, Object> resourceAccess = new HashMap<>();
    resourceAccess.put("druidms", druidms);

    String at = new PlainJWT(new JWTClaimsSet.Builder()
                                 .claim("resource_access", resourceAccess)
                                 .build()).serialize();

    OidcProfile profile = new OidcProfile();
    profile.setAccessToken(new BearerAccessToken(at, 3600L, null));

    RoleBasedAuthGen gen = new RoleBasedAuthGen("resource_access.druidms.roles");
    gen.generate(null, null, profile);

    assertEquals(new HashSet<>(Arrays.asList("admin", "user")), profile.getRoles());
  }

  @Test
  public void supportsSingleStringLeaf()
  {
    Map<String, Object> realmAccess = new HashMap<>();
    realmAccess.put("roles", "admin");

    String at = new PlainJWT(new JWTClaimsSet.Builder()
                                 .claim("realm_access", realmAccess)
                                 .build()).serialize();

    OidcProfile profile = new OidcProfile();
    profile.setAccessToken(new BearerAccessToken(at, 3600L, null));

    RoleBasedAuthGen gen = new RoleBasedAuthGen("realm_access.roles");
    gen.generate(null, null, profile);

    assertEquals(new HashSet<>(Collections.singletonList("admin")), profile.getRoles());
  }

  @Test
  public void noAccessTokenLeavesRolesEmpty()
  {
    OidcProfile profile = new OidcProfile();

    RoleBasedAuthGen gen = new RoleBasedAuthGen("resource_access.druidms.roles");
    gen.generate(null, null, profile);

    assertTrue(profile.getRoles().isEmpty());
  }
}
