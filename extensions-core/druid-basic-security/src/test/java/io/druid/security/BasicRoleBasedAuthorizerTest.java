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

package io.druid.security;

import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.db.TestDerbyAuthorizerStorageConnector;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

public class BasicRoleBasedAuthorizerTest
{
  private static final String TEST_DB_PREFIX = "test";

  @Rule
  public final TestDerbyAuthorizerStorageConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyAuthorizerStorageConnector.DerbyConnectorRule(TEST_DB_PREFIX);

  private BasicRoleBasedAuthorizer authorizer;
  private TestDerbyAuthorizerStorageConnector connector;

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    createAllTables();
    authorizer = new BasicRoleBasedAuthorizer(connector, TEST_DB_PREFIX, 5000);
  }

  @Test
  public void testAuth()
  {
    connector.createUser(TEST_DB_PREFIX, "druid");
    connector.createRole(TEST_DB_PREFIX, "druidRole");
    connector.assignRole(TEST_DB_PREFIX, "druid", "druidRole");

    ResourceAction permission = new ResourceAction(
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    connector.addPermission(TEST_DB_PREFIX, "druidRole", permission);

    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null);

    Access access = authorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = authorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testMorePermissionsThanCacheSize()
  {
    connector.createUser(TEST_DB_PREFIX, "druid");
    connector.createRole(TEST_DB_PREFIX, "druidRole");
    connector.assignRole(TEST_DB_PREFIX, "druid", "druidRole");

    for (int i = 0; i < authorizer.getPermissionCacheSize() + 50; i++) {
      ResourceAction permission = new ResourceAction(
          new Resource("testResource-" + i, ResourceType.DATASOURCE),
          Action.WRITE
      );
      connector.addPermission(TEST_DB_PREFIX, "druidRole", permission);
    }

    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null);

    Access access = authorizer.authorize(
        authenticationResult,
        new Resource("testResource-300", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = authorizer.authorize(
        authenticationResult,
        new Resource("matchesNothing", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @After
  public void tearDown() throws Exception
  {
    dropAllTables();
  }

  private void createAllTables()
  {
    connector.createUserTable(TEST_DB_PREFIX);
    connector.createRoleTable(TEST_DB_PREFIX);
    connector.createPermissionTable(TEST_DB_PREFIX);
    connector.createUserRoleTable(TEST_DB_PREFIX);
  }

  private void dropAllTables()
  {
    for (String table : connector.getTableNames(TEST_DB_PREFIX)) {
      dropTable(table);
    }
  }

  private void dropTable(final String tableName)
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }
}
