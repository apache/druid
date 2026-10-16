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

import org.apache.druid.security.basic.authorization.RoleProviderUtil;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class RoleProviderUtilTest
{

  @Test
  public void getRolesByIdentityAddsRolesWhenUserFound()
  {
    Set<String> roles = Set.of("r1", "r2");
    BasicAuthorizerUser user = new BasicAuthorizerUser("id", roles);

    Map<String, BasicAuthorizerUser> userMap = Map.of("id", user);

    Set<String> out = RoleProviderUtil.getRolesByIdentity(userMap, "id");
    assertEquals(roles, out);
  }

  @Test
  public void getRolesByIdentityNoopWhenUserMissing()
  {
    Map<String, BasicAuthorizerUser> userMap = Map.of();
    Set<String> out = RoleProviderUtil.getRolesByIdentity(userMap, "missing");
    assertTrue(out.isEmpty());
  }

  @Test
  public void getRolesByClaimValuesFiltersByRoleNames()
  {
    Map<String, BasicAuthorizerRole> roles = new HashMap<>();
    roles.put("r1", null);
    roles.put("r2", null);

    BasicAuthorizerCacheManager cache = new StubCacheManager(Map.of(), roles);

    Set<String> claims = Set.of("r2", "nope");
    Set<String> out = RoleProviderUtil.getRolesByClaimValue("authz", claims, cache);
    assertEquals(Set.of("r2"), out);
  }

  @Test
  public void getRolesByClaimValuesThrowsWhenRoleMapNull()
  {
    BasicAuthorizerCacheManager cache = new StubCacheManager(Map.of(), null);
    assertTrue(RoleProviderUtil.getRolesByClaimValue("authz", Set.of("r2"), cache).isEmpty());
  }
}
