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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.java.util.common.IAE;
import io.druid.security.basic.db.BasicSecurityStorageConnector;
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
  private final BasicSecurityStorageConnector dbConnector;

  @JsonCreator
  public BasicRoleBasedAuthorizer(
      @JacksonInject BasicSecurityStorageConnector dbConnector
  )
  {
    this.dbConnector = dbConnector;
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult, Resource resource, Action action
  )
  {
    if (authenticationResult == null) {
      throw new IAE("WTF? authenticationResult should never be null.");
    }

    List<Map<String, Object>> permissions = dbConnector.getPermissionsForUser(authenticationResult.getIdentity());

    // maybe optimize this later
    for (Map<String, Object> permission : permissions) {
      if (permissionCheck(resource, action, permission)) {
        return new Access(true);
      }
    }

    return new Access(false);
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
    Pattern resourceNamePattern = Pattern.compile(permissionResourceName);
    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }
}
