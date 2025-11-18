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
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
}
