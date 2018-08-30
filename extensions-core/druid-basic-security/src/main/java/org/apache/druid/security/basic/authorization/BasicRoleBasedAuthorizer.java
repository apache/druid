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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.security.basic.BasicAuthDBConfig;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("basic")
public class BasicRoleBasedAuthorizer implements Authorizer
{
  private final BasicAuthorizerCacheManager cacheManager;
  private final String name;
  private final BasicAuthDBConfig dbConfig;


  @JsonCreator
  public BasicRoleBasedAuthorizer(
      @JacksonInject BasicAuthorizerCacheManager cacheManager,
      @JsonProperty("name") String name,
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout
  )
  {
    this.name = name;
    this.cacheManager = cacheManager;
    this.dbConfig = new BasicAuthDBConfig(
        null,
        null,
        enableCacheNotifications == null ? true : enableCacheNotifications,
        cacheNotificationTimeout == null ? BasicAuthDBConfig.DEFAULT_CACHE_NOTIFY_TIMEOUT_MS : cacheNotificationTimeout,
        0
    );
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult, Resource resource, Action action
  )
  {
    if (authenticationResult == null) {
      throw new IAE("WTF? authenticationResult should never be null.");
    }

    Map<String, BasicAuthorizerUser> userMap = cacheManager.getUserMap(name);
    if (userMap == null) {
      throw new IAE("Could not load userMap for authorizer [%s]", name);
    }

    Map<String, BasicAuthorizerRole> roleMap = cacheManager.getRoleMap(name);
    if (roleMap == null) {
      throw new IAE("Could not load roleMap for authorizer [%s]", name);
    }

    BasicAuthorizerUser user = userMap.get(authenticationResult.getIdentity());
    if (user == null) {
      return new Access(false);
    }

    for (String roleName : user.getRoles()) {
      BasicAuthorizerRole role = roleMap.get(roleName);
      for (BasicAuthorizerPermission permission : role.getPermissions()) {
        if (permissionCheck(resource, action, permission)) {
          return new Access(true);
        }
      }
    }

    return new Access(false);
  }

  private boolean permissionCheck(Resource resource, Action action, BasicAuthorizerPermission permission)
  {
    if (action != permission.getResourceAction().getAction()) {
      return false;
    }

    Resource permissionResource = permission.getResourceAction().getResource();
    if (permissionResource.getType() != resource.getType()) {
      return false;
    }

    Pattern resourceNamePattern = permission.getResourceNamePattern();
    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }

  public BasicAuthDBConfig getDbConfig()
  {
    return dbConfig;
  }
}
