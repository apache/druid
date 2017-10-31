/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic.authorization;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.druid.java.util.common.IAE;
import io.druid.security.basic.db.BasicAuthDBConfig;
import io.druid.security.basic.db.BasicAuthorizerStorageConnector;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authorizer;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("basic")
public class BasicRoleBasedAuthorizer implements Authorizer
{
  private static final int DEFAULT_PERMISSION_CACHE_SIZE = 5000;
  private final BasicAuthorizerStorageConnector dbConnector;
  private final LoadingCache<String, Pattern> permissionPatternCache;
  private final int permissionCacheSize;
  private final BasicAuthDBConfig dbConfig;

  @JsonCreator
  public BasicRoleBasedAuthorizer(
      @JacksonInject BasicAuthorizerStorageConnector dbConnector,
      @JsonProperty("dbPrefix") String dbPrefix,
      @JsonProperty("permissionCacheSize") Integer permissionCacheSize
  )
  {
    this.dbConfig = new BasicAuthDBConfig(dbPrefix, null, null);
    this.dbConnector = dbConnector;
    this.permissionCacheSize = permissionCacheSize == null ? DEFAULT_PERMISSION_CACHE_SIZE : permissionCacheSize;
    this.permissionPatternCache = Caffeine.newBuilder()
                                          .maximumSize(this.permissionCacheSize)
                                          .build(regexStr -> Pattern.compile(regexStr));
  }

  /**
   * constructor for unit tests
   */
  public BasicRoleBasedAuthorizer(
      BasicAuthorizerStorageConnector dbConnector,
      String dbPrefix,
      int permissionCacheSize
  )
  {
    this.dbConnector = dbConnector;
    this.permissionCacheSize = permissionCacheSize;
    this.permissionPatternCache = Caffeine.newBuilder()
                                          .maximumSize(this.permissionCacheSize)
                                          .build(regexStr -> Pattern.compile(regexStr));
    this.dbConfig = new BasicAuthDBConfig(dbPrefix, null, null);
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult, Resource resource, Action action
  )
  {
    if (authenticationResult == null) {
      throw new IAE("WTF? authenticationResult should never be null.");
    }

    List<Map<String, Object>> permissions = dbConnector.getPermissionsForUser(dbConfig.getDbPrefix(), authenticationResult.getIdentity());

    for (Map<String, Object> permission : permissions) {
      if (permissionCheck(resource, action, permission)) {
        return new Access(true);
      }
    }

    return new Access(false);
  }

  public BasicAuthorizerStorageConnector getDbConnector()
  {
    return dbConnector;
  }

  public String getDBPrefix()
  {
    return dbConfig.getDbPrefix();
  }

  private boolean permissionCheck(Resource resource, Action action, Map<String, Object> permission)
  {
    ResourceAction permissionResourceAction = (ResourceAction) permission.get("resourceAction");
    if (action != permissionResourceAction.getAction()) {
      return false;
    }

    Resource permissionResource = permissionResourceAction.getResource();
    if (permissionResource.getType() != resource.getType()) {
      return false;
    }

    String permissionResourceName = permissionResource.getName();

    Pattern resourceNamePattern = permissionPatternCache.get(permissionResourceName);
    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }

  public int getPermissionCacheSize()
  {
    return permissionCacheSize;
  }

  public BasicAuthDBConfig getDbConfig()
  {
    return dbConfig;
  }
}
