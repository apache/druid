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

package org.apache.druid.security.basic.authorization;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.server.security.AuthenticationResult;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RoleProviderUtil
{
  public static final String ROLE_CLAIM_CONTEXT_KEY = "druidRoles";

  public static Set<String> getUserRoles(
      Map<String, BasicAuthorizerUser> userMap,
      String authorizerPrefix,
      AuthenticationResult authenticationResult,
      BasicAuthorizerCacheManager cacheManager
  )
  {
    Set<String> claims = RoleProviderUtil.claimValuesFromCtx(authenticationResult.getContext());

    if (claims != null) {
      return getRolesByClaimValue(
          authorizerPrefix,
          claims,
          cacheManager
      );
    } else {
      return getRolesByIdentity(
          userMap,
          authenticationResult.getIdentity()
      );
    }
  }

  @VisibleForTesting
  public static Set<String> getRolesByIdentity(
      Map<String, BasicAuthorizerUser> userMap,
      String identity
  )
  {
    Set<String> roles = new HashSet<>();

    BasicAuthorizerUser user = userMap.get(identity);
    if (user != null) {
      roles.addAll(user.getRoles());
    }
    return roles;
  }

  @VisibleForTesting
  public static Set<String> getRolesByClaimValue(
      String authorizerPrefix,
      Set<String> claimValue,
      BasicAuthorizerCacheManager cacheManager
  )
  {
    Map<String, BasicAuthorizerRole> roleMap = cacheManager.getRoleMap(authorizerPrefix);

    if (roleMap == null) {
      return Set.of();
    }

    Set<String> roles = new HashSet<>();

    roleMap.keySet()
           .stream()
           .filter(claimValue::contains)
           .forEach(roles::add);

    return roles;
  }

  @Nullable
  protected static Set<String> claimValuesFromCtx(Map<String, Object> ctx)
  {
    Object value = (ctx == null) ? null : ctx.get(RoleProviderUtil.ROLE_CLAIM_CONTEXT_KEY);
    if (!(value instanceof Set)) {
      return null;
    }
    Set<?> rawClaimValues = (Set<?>) value;

    Set<String> result = new HashSet<>();
    for (Object claimValue : rawClaimValues) {
      if (!(claimValue instanceof String)) {
        return null;
      }
      String str = ((String) claimValue).trim();
      if (!str.isEmpty()) {
        result.add(str);
      }
    }
    return result.isEmpty() ? null : result;
  }
}
