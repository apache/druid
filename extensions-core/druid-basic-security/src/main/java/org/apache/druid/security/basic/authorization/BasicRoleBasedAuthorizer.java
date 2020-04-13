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
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("basic")
public class BasicRoleBasedAuthorizer implements Authorizer
{
  private final String name;
  private final BasicAuthDBConfig dbConfig;
  private final RoleProvider roleProvider;

  @JsonCreator
  public BasicRoleBasedAuthorizer(
      @JacksonInject BasicAuthorizerCacheManager cacheManager,
      @JsonProperty("name") String name,
      @JsonProperty("initialAdminUser") String initialAdminUser,
      @JsonProperty("initialAdminRole") String initialAdminRole,
      @JsonProperty("initialAdminGroupMapping") String initialAdminGroupMapping,
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout,
      @JsonProperty("roleProvider") RoleProvider roleProvider
  )
  {
    this.name = name;
    this.dbConfig = new BasicAuthDBConfig(
        null,
        null,
        initialAdminUser,
        initialAdminRole,
        initialAdminGroupMapping,
        enableCacheNotifications == null ? true : enableCacheNotifications,
        cacheNotificationTimeout == null ? BasicAuthDBConfig.DEFAULT_CACHE_NOTIFY_TIMEOUT_MS : cacheNotificationTimeout,
        0
    );
    if (roleProvider == null) {
      this.roleProvider = new MetadataStoreRoleProvider(cacheManager);
    } else {
      this.roleProvider = roleProvider;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    if (authenticationResult == null) {
      throw new IAE("authenticationResult is null where it should never be.");
    }

    Set<String> roleNames = new HashSet<>(roleProvider.getRoles(name, authenticationResult));
    Map<String, BasicAuthorizerRole> roleMap = roleProvider.getRoleMap(name);

    if (roleNames.isEmpty()) {
      return new Access(false);
    }
    if (roleMap == null) {
      throw new IAE("Could not load roleMap for authorizer [%s]", name);
    }

    for (String roleName : roleNames) {
      BasicAuthorizerRole role = roleMap.get(roleName);
      if (role != null) {
        for (BasicAuthorizerPermission permission : role.getPermissions()) {
          if (permissionCheck(resource, action, permission)) {
            return new Access(true);
          }
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
