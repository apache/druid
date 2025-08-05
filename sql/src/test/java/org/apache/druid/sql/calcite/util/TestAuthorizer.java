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

package org.apache.druid.sql.calcite.util;

import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;

import java.util.Optional;
import java.util.Set;

public class TestAuthorizer
{
  /**
   * Creates a simple authorizer that allows access based on the following rules:
   * <li>resources of type DATASOURCE with names containing "restricted" for read include a policy restriction</li>
   * <li>superuser has full access</li>
   * <li>resources with names containing "forbidden" are denied</li>
   * <li>external resources are denied for read actions</li>
   * <li>resources of type DATASOURCE, VIEW, QUERY_CONTEXT, and EXTERNAL are allowed</li>
   * <li>if none of the roles above matches, deny access</li>
   */
  public static Authorizer simple(String superuserName, Policy defaultPolicy)
  {
    return (authenticationResult, resource, action) ->
        new TestAuthorizer(authenticationResult, resource, action)
            .defaultPolicyOnReadTable(defaultPolicy)
            .allowIfSuperuser(superuserName)
            .denyIfResourceNameHasKeyword("forbidden")
            .denyExternalRead()
            .allowIfResourceTypeIs(Set.of(
                ResourceType.DATASOURCE,
                ResourceType.VIEW,
                ResourceType.QUERY_CONTEXT,
                ResourceType.EXTERNAL
            ))
            .access()
            .orElse(Access.DENIED);
  }

  AuthenticationResult authenticationResult;
  Resource resource;
  Action action;
  Optional<Access> access;

  String superuserName;
  Policy defaultPolicyRestriction;

  public TestAuthorizer(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    this.authenticationResult = authenticationResult;
    this.resource = resource;
    this.action = action;
    this.access = Optional.empty();
  }

  public TestAuthorizer defaultPolicyOnReadTable(Policy defaultPolicy)
  {
    this.defaultPolicyRestriction = defaultPolicy;
    return this;
  }

  public TestAuthorizer allowIfSuperuser(String superuserName)
  {
    this.superuserName = superuserName;
    if (access.isPresent()) {
      return this;
    }
    if (superuserName.equals(authenticationResult.getIdentity())) {
      access = Optional.of(allow(resource, action, authenticationResult.getIdentity()));
    }
    return this;
  }

  public TestAuthorizer denyIfResourceNameHasKeyword(String keyword)
  {
    if (access.isPresent()) {
      return this;
    }
    if (resource.getName().contains(keyword)) {
      access = Optional.of(Access.DENIED);
    }
    return this;
  }

  public TestAuthorizer denyExternalRead()
  {
    if (access.isPresent()) {
      return this;
    }
    if (ResourceType.EXTERNAL.equals(resource.getType()) && Action.READ.equals(action)) {
      access = Optional.of(Access.DENIED);
    }
    return this;
  }

  public TestAuthorizer allowIfResourceTypeIs(Set<String> resourceTypes)
  {
    if (access.isPresent()) {
      return this;
    }
    if (resourceTypes.contains(resource.getType())) {
      access = Optional.of(allow(resource, action, authenticationResult.getIdentity()));
    }
    return this;
  }

  public TestAuthorizer allowResourceAccessForUsers(String resourceType, Action resourceAction, Set<String> users)
  {
    if (access.isPresent()) {
      return this;
    }
    if (resource.getType().equals(resourceType)
        && action.equals(resourceAction)
        && users.contains(authenticationResult.getIdentity())) {
      access = Optional.of(allow(resource, action, authenticationResult.getIdentity()));
    }
    return this;
  }

  public Optional<Access> access()
  {
    return access;
  }


  private Access allow(Resource resource, Action action, String user)
  {
    if (defaultPolicyRestriction == null) {
      return Access.OK;
    }

    Policy policy = user.equals(superuserName) ? NoRestrictionPolicy.instance() : defaultPolicyRestriction;
    boolean readRestrictedTable = resource.getType().equals(ResourceType.DATASOURCE)
                                  && resource.getName().contains("restricted")
                                  && action.equals(Action.READ);
    return readRestrictedTable ? Access.allowWithRestriction(policy) : Access.OK;
  }
}
