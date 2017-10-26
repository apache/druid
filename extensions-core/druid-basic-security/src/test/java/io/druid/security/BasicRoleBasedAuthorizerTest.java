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
import io.druid.security.basic.BasicAuthConfig;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.db.SQLBasicSecurityStorageConnector;
import io.druid.security.db.TestDerbySecurityConnector;
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
  @Rule
  public final TestDerbySecurityConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbySecurityConnector.DerbyConnectorRule();

  private BasicRoleBasedAuthorizer authorizer;
  private TestDerbySecurityConnector connector;
  private BasicAuthConfig authConfig;

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    createAllTables();
    authConfig = new BasicAuthConfig() {

      @Override
      public int getPermissionCacheSize()
      {
        return 500;
      }
    };
    authorizer = new BasicRoleBasedAuthorizer(connector, authConfig);
  }

  @Test
  public void testAuth()
  {
    connector.createUser("druid");
    connector.createRole("druidRole");
    connector.assignRole("druid", "druidRole");

    ResourceAction permission = new ResourceAction(
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    connector.addPermission("druidRole", permission);

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
    connector.createUser("druid");
    connector.createRole("druidRole");
    connector.assignRole("druid", "druidRole");

    for (int i = 0; i < authConfig.getPermissionCacheSize() + 50; i++) {
      ResourceAction permission = new ResourceAction(
          new Resource("testResource-" + i, ResourceType.DATASOURCE),
          Action.WRITE
      );
      connector.addPermission("druidRole", permission);
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
    connector.createUserTable();
    connector.createRoleTable();
    connector.createPermissionTable();
    connector.createUserRoleTable();
    connector.createUserCredentialsTable();
  }

  private void dropAllTables()
  {
    for (String table : SQLBasicSecurityStorageConnector.TABLE_NAMES) {
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
