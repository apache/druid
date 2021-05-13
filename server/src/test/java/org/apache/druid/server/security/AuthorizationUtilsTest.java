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
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    Function<String, Iterable<ResourceAction>> resourceActionGenerator = new Function<String, Iterable<ResourceAction>>()
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
    authorizerMap.put(authorizerName, new AllowAllAuthorizer());

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
}
