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

package org.apache.druid.security.ranger.authorizer;

import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RangerAuthorizerTest
{
  static RangerAuthorizer rangerAuthorizer = null;

  private static final AuthenticationResult alice = new AuthenticationResult("alice", null, null, null);
  private static final AuthenticationResult bob = new AuthenticationResult("bob", null, null, null);

  private static final Resource aliceDatasource = new Resource("alice-datasource", ResourceType.DATASOURCE);
  private static final Resource aliceConfig = new Resource("config", ResourceType.CONFIG);
  private static final Resource aliceState = new Resource("state", ResourceType.STATE);

  @BeforeClass
  public static void setupBeforeClass()
  {
    rangerAuthorizer = new RangerAuthorizer(null, null, false, new Configuration());
  }

  @Test
  public void testOperations()
  {
    Assert.assertTrue(rangerAuthorizer.authorize(alice, aliceDatasource, Action.READ).isAllowed());
    Assert.assertTrue(rangerAuthorizer.authorize(alice, aliceDatasource, Action.READ).isAllowed());
    Assert.assertTrue(rangerAuthorizer.authorize(alice, aliceConfig, Action.READ).isAllowed());
    Assert.assertTrue(rangerAuthorizer.authorize(alice, aliceConfig, Action.WRITE).isAllowed());
    Assert.assertTrue(rangerAuthorizer.authorize(alice, aliceState, Action.READ).isAllowed());
    Assert.assertTrue(rangerAuthorizer.authorize(alice, aliceState, Action.WRITE).isAllowed());

    Assert.assertFalse(rangerAuthorizer.authorize(bob, aliceDatasource, Action.READ).isAllowed());
  }
}
