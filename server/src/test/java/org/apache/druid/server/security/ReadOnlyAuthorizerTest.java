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

import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.junit.Assert;
import org.junit.Test;

public class ReadOnlyAuthorizerTest
{

  @Test
  public void testAuth()
  {
    ReadOnlyAuthorizer authorizer = new ReadOnlyAuthorizer(null);
    AuthenticationResult authenticationResult = new AuthenticationResult("anonymous", "anonymous", null, null);
    Access access = authorizer.authorize(
            authenticationResult,
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
    access = authorizer.authorize(
            authenticationResult,
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.READ
    );
    Assert.assertTrue(access.isAllowed());
  }

  @Test
  public void testAuthWithPolicy()
  {
    RowFilterPolicy policy = RowFilterPolicy.from(new NullFilter("column", null));
    ReadOnlyAuthorizer authorizerWithPolicy = new ReadOnlyAuthorizer(policy);
    AuthenticationResult authenticationResult = new AuthenticationResult("anonymous", "anonymous", null, null);

    // READ on DATASOURCE should return policy restriction
    Access access = authorizerWithPolicy.authorize(
            authenticationResult,
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.READ
    );
    Assert.assertTrue(access.isAllowed());
    Assert.assertTrue(access.getPolicy().isPresent());
    Assert.assertEquals(policy, access.getPolicy().get());

    // WRITE should still be denied
    access = authorizerWithPolicy.authorize(
            authenticationResult,
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    // READ on non-DATASOURCE should not have policy
    access = authorizerWithPolicy.authorize(
            authenticationResult,
            new Resource("testResource", ResourceType.STATE),
            Action.READ
    );
    Assert.assertTrue(access.isAllowed());
    Assert.assertFalse(access.getPolicy().isPresent());
  }
}
