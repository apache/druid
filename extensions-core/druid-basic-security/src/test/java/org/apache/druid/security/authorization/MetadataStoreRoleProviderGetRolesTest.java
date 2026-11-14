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

package org.apache.druid.security.authorization;

import org.apache.druid.security.basic.authorization.MetadataStoreRoleProvider;
import org.apache.druid.security.basic.authorization.RoleProviderUtil;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MetadataStoreRoleProviderGetRolesTest
{

  @Test
  public void returnsRolesByClaimValuesWhenPresent()
  {
    Map<String, BasicAuthorizerRole> roles = new HashMap<>();
    roles.put("admin", null);
    roles.put("viewer", null);

    Set<String> viewerRole = Set.of("viewer");

    BasicAuthorizerUser user = new BasicAuthorizerUser("alice", viewerRole);

    Map<String, BasicAuthorizerUser> users = Map.of("alice", user);

    BasicAuthorizerCacheManager cache = new StubCacheManager(users, roles);
    MetadataStoreRoleProvider provider = new MetadataStoreRoleProvider(cache);

    Set<String> claims = Set.of("admin", "extraneous");

    Map<String, Object> ctx = Map.of(RoleProviderUtil.ROLE_CLAIM_CONTEXT_KEY, claims);

    AuthenticationResult ar = new AuthenticationResult("alice", "basic", "pac4j", ctx);

    Set<String> out = provider.getRoles("basic", ar);
    Set<String> expected = Set.of("admin");
    assertEquals(expected, out);
  }

  @Test
  public void fallsBackToIdentityWhenNoClaimContext()
  {
    Set<String> viewerRole = Set.of("viewer");
    BasicAuthorizerUser user = new BasicAuthorizerUser("alice", viewerRole);

    Map<String, BasicAuthorizerUser> users = Map.of("alice", user);

    Map<String, BasicAuthorizerRole> roles = new HashMap<>();
    roles.put("admin", null);

    BasicAuthorizerCacheManager cache = new StubCacheManager(users, roles);
    MetadataStoreRoleProvider provider = new MetadataStoreRoleProvider(cache);

    AuthenticationResult ar = new AuthenticationResult("alice", "basic", "pac4j", Collections.emptyMap());

    Set<String> out = provider.getRoles("basic", ar);
    Set<String> expected = Set.of("viewer");
    assertEquals(expected, out);
  }
}
