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

package org.apache.druid.server.security;

import com.google.common.base.Function;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class AuthorizationUtilsTest
{

  @Test
  public void testFilterResourcesWithAllowAllAuthorizer()
  {
    String authorizerName = "allowAll";
    AuthenticationResult authenticationResult = new AuthenticationResult(
        "identity",
        authorizerName,
        "authenticator",
        null
    );
    List<String> resources = new ArrayList<>();
    resources.add("duplicate");
    resources.add("duplicate");
    resources.add("filteredResource");
    resources.add("hello");

    Function<String, Iterable<ResourceAction>> resourceActionGenerator = new Function<>()
    {
      @Nullable
      @Override
      public Iterable<ResourceAction> apply(@Nullable String input)
      {
        if ("filteredResource".equals(input)) {
          return null;
        }
        return Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ));
      }
    };

    Map<String, Authorizer> authorizerMap = new HashMap<>();
    authorizerMap.put(authorizerName, new AllowAllAuthorizer(null));

    AuthorizerMapper mapper = new AuthorizerMapper(authorizerMap);

    Iterable<String> filteredResults = AuthorizationUtils.filterAuthorizedResources(
        authenticationResult,
        resources,
        resourceActionGenerator,
        mapper
    );

    Iterator<String> itr = filteredResults.iterator();
    // Validate that resource "filteredResource" is not present because null was returned for it.
    // Also validate that calling the filterAuthorizedResources method doesn't get rid of duplicate resources
    Assert.assertTrue(itr.hasNext());
    Assert.assertEquals("duplicate", itr.next());
    Assert.assertEquals("duplicate", itr.next());
    Assert.assertEquals("hello", itr.next());
    Assert.assertFalse(itr.hasNext());
  }

  @Test
  public void testMakeSuperuserPermissions()
  {
    final String customType = "CUSTOM";
    ResourceType.registerResourceType(customType);
    final List<ResourceAction> permissions = AuthorizationUtils.makeSuperUserPermissions();
    // every type and action should have a wildcard pattern
    for (String type : ResourceType.knownTypes()) {
      for (Action action : Action.values()) {
        Assert.assertTrue(
            permissions.stream()
                       .filter(ra -> Objects.equals(type, ra.getResource().getType()))
                       .anyMatch(ra -> action == ra.getAction() && ".*".equals(ra.getResource().getName()))
        );
      }
    }
    // custom type should be there too
    for (Action action : Action.values()) {
      Assert.assertTrue(
          permissions.stream()
                     .filter(ra -> Objects.equals(customType, ra.getResource().getType()))
                     .anyMatch(ra -> action == ra.getAction() && ".*".equals(ra.getResource().getName()))
      );
    }
  }

  @Test
  public void testAuthorizationAttributeIfNeeded()
  {
    MockHttpServletRequest request = new MockHttpServletRequest();
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    Assert.assertEquals(true, request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED));
  }

  @Test
  public void testShouldApplyPolicy_readDatasource()
  {
    final Resource datasourceResource = new Resource("test", ResourceType.DATASOURCE);
    Assert.assertTrue(AuthorizationUtils.shouldApplyPolicy(datasourceResource, Action.READ));
  }

  @Test
  public void testShouldApplyPolicy_writeDatasource()
  {
    final Resource datasourceResource = new Resource("test", ResourceType.DATASOURCE);
    Assert.assertFalse(AuthorizationUtils.shouldApplyPolicy(datasourceResource, Action.WRITE));
  }

  @Test
  public void testShouldApplyPolicy_readState()
  {
    Assert.assertFalse(AuthorizationUtils.shouldApplyPolicy(Resource.STATE_RESOURCE, Action.READ));
  }

  @Test
  public void testShouldApplyPolicy_writeState()
  {
    Assert.assertFalse(AuthorizationUtils.shouldApplyPolicy(Resource.STATE_RESOURCE, Action.WRITE));
  }

  @Test
  public void testShouldApplyPolicy_readExternal()
  {
    final Resource externalResource = new Resource("test", ResourceType.EXTERNAL);
    Assert.assertFalse(AuthorizationUtils.shouldApplyPolicy(externalResource, Action.READ));
  }

  @Test
  public void testShouldApplyPolicy_writeExternal()
  {
    final Resource externalResource = new Resource("test", ResourceType.EXTERNAL);
    Assert.assertFalse(AuthorizationUtils.shouldApplyPolicy(externalResource, Action.WRITE));
  }

  @Test
  public void testAuthorizeAllResourceActions_sameDatasourceReadAndWrite()
  {
    // Verifies that having both READ and WRITE on the same datasource works correctly.
    final String authorizerName = "testAuthorizer";
    final AuthenticationResult authenticationResult = new AuthenticationResult(
        "identity",
        authorizerName,
        "authenticator",
        null
    );

    // Create an authorizer that returns a policy for datasource reads
    final RowFilterPolicy policy = RowFilterPolicy.from(new EqualityFilter("foo", ColumnType.STRING, "bar", null));
    final Authorizer authorizer = (authResult, resource, action) -> {
      if (ResourceType.DATASOURCE.equals(resource.getType()) && Action.READ.equals(action)) {
        return Access.allowWithRestriction(policy);
      }
      return Access.OK;
    };

    final Map<String, Authorizer> authorizerMap = new HashMap<>();
    authorizerMap.put(authorizerName, authorizer);
    final AuthorizerMapper mapper = new AuthorizerMapper(authorizerMap);

    // Both READ and WRITE on same datasource
    final List<ResourceAction> resourceActions = Arrays.asList(
        new ResourceAction(new Resource("test", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("test", ResourceType.DATASOURCE), Action.WRITE)
    );

    // This should succeed without throwing an exception
    final AuthorizationResult result = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        resourceActions,
        mapper
    );

    Assert.assertTrue(result.allowBasicAccess());
    // Verify that the policy was captured for the READ action
    Assert.assertEquals(Map.of("test", Optional.of(policy)), result.getPolicyMap());
  }

  @Test
  public void testAuthorizeAllResourceActions_policyForNonReadDatasourceThrows()
  {
    // Verifies that if an authorizer incorrectly returns a policy for a non-read action,
    // a defensive exception is thrown.
    final String authorizerName = "testAuthorizer";
    final AuthenticationResult authenticationResult =
        new AuthenticationResult("identity", authorizerName, "authenticator", null);

    // Create a broken authorizer that returns a policy for WRITE actions
    final Authorizer authorizer = (authResult, resource, action) -> {
      if (ResourceType.DATASOURCE.equals(resource.getType()) && Action.WRITE.equals(action)) {
        // This is incorrect - policies should only be returned for READ actions on tables
        return Access.allowWithRestriction(NoRestrictionPolicy.instance());
      }
      return Access.OK;
    };

    final Map<String, Authorizer> authorizerMap = new HashMap<>();
    authorizerMap.put(authorizerName, authorizer);
    final AuthorizerMapper mapper = new AuthorizerMapper(authorizerMap);

    final List<ResourceAction> resourceActions = Collections.singletonList(
        new ResourceAction(new Resource("test", ResourceType.DATASOURCE), Action.WRITE)
    );

    final DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> AuthorizationUtils.authorizeAllResourceActions(authenticationResult, resourceActions, mapper)
    );
    Assert.assertTrue(exception.getMessage().contains("Policy should only present when reading a table"));
  }
}
