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
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;

import java.util.Optional;
import java.util.Set;

public class TestAuthorizer
{
  public static final String TEST_SUPERUSER_NAME = "testSuperuser";
  public static final Policy POLICY_RESTRICTION = RowFilterPolicy.from(BaseCalciteQueryTest.equality(
      "m1",
      6,
      ColumnType.LONG
  ));

  public static Authorizer simple()
  {
    return (authenticationResult, resource, action) ->
        new TestAuthorizer(authenticationResult, resource, action)
            .allowIfSuperuser()
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

  public static Authorizer simple2()
  {
    return (authenticationResult, resource, action) ->
        new TestAuthorizer(authenticationResult, resource, action)
            .allowIfSuperuser()
            .denyIfResourceNameHasKeyword("forbidden")
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

  TestAuthorizer(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    this.authenticationResult = authenticationResult;
    this.resource = resource;
    this.action = action;
    this.access = Optional.empty();
  }

  TestAuthorizer allowIfSuperuser()
  {
    if (access.isPresent()) {
      return this;
    }
    if (TEST_SUPERUSER_NAME.equals(authenticationResult.getIdentity())) {
      access = Optional.of(allow(resource, action, authenticationResult.getIdentity()));
    }
    return this;
  }


  TestAuthorizer denyIfResourceNameHasKeyword(String keyword)
  {
    if (access.isPresent()) {
      return this;
    }
    if (resource.getName().contains(keyword)) {
      access = Optional.of(Access.DENIED);
    }
    return this;
  }

  TestAuthorizer denyExternalRead()
  {
    if (access.isPresent()) {
      return this;
    }
    if (ResourceType.EXTERNAL.equals(resource.getType()) && Action.READ.equals(action)) {
      access = Optional.of(Access.DENIED);
    }
    return this;
  }

  TestAuthorizer allowIfResourceTypeIs(Set<String> resourceTypes)
  {
    if (access.isPresent()) {
      return this;
    }
    if (resourceTypes.contains(resource.getType())) {
      access = Optional.of(allow(resource, action, authenticationResult.getIdentity()));
    }
    return this;
  }

  Optional<Access> access()
  {
    return access;
  }


  private static Access allow(Resource resource, Action action, String user)
  {
    Policy policy = TEST_SUPERUSER_NAME.equals(user) ? NoRestrictionPolicy.instance() : POLICY_RESTRICTION;
    boolean readRestrictedTable = resource.getType().equals(ResourceType.DATASOURCE)
                                  && resource.getName().contains("restricted")
                                  && action.equals(Action.READ);
    return readRestrictedTable ? Access.allowWithRestriction(policy) : Access.OK;
  }
}
