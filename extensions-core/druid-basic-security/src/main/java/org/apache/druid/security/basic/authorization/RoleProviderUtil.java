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

import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RoleProviderUtil
{
  public static final String ROLE_CLAIM_CONTEXT_KEY = "druidRoles";

  public static Set<String> getRolesByIdentity(
      Map<String, BasicAuthorizerUser> userMap,
      String identity,
      Set<String> roleNames
  )
  {
    BasicAuthorizerUser user = userMap.get(identity);
    if (user != null) {
      roleNames.addAll(user.getRoles());
    }
    return roleNames;
  }

  public static Set<String> getRolesByClaimValue(
      String authorizerPrefix,
      Set<String> claimValue,
      Set<String> roleNames,
      BasicAuthorizerCacheManager cacheManager
  )
  {
    Map<String, BasicAuthorizerRole> roleMap = cacheManager.getRoleMap(authorizerPrefix);

    if (roleMap == null) {
      return Set.of();
    }

    roleMap.keySet()
           .stream()
           .filter(claimValue::contains)
           .forEach(roleNames::add);

    return roleNames;
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
