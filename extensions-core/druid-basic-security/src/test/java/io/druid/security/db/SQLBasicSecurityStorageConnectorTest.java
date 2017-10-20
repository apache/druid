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
package io.druid.security.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.db.SQLBasicSecurityStorageConnector;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;

public class SQLBasicSecurityStorageConnectorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbySecurityConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbySecurityConnector.DerbyConnectorRule();

  private TestDerbySecurityConnector connector;

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    createAllTables();
  }

  @After
  public void tearDown() throws Exception
  {
    dropAllTables();
  }

  @Test
  public void testCreateTables() throws Exception
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            for (String table : SQLBasicSecurityStorageConnector.TABLE_NAMES) {
              Assert.assertTrue(
                  StringUtils.format("table %s was not created!", table),
                  connector.tableExists(handle, table)
              );
            }

            return null;
          }
        }
    );
  }

  // user tests
  @Test
  public void testCreateDeleteUser() throws Exception
  {
    connector.createUser("druid");
    Map<String, Object> expectedUser = ImmutableMap.of(
        "name", "druid"
    );
    Map<String, Object> dbUser = connector.getUser("druid");
    Assert.assertEquals(expectedUser, dbUser);

    connector.deleteUser("druid");
    dbUser = connector.getUser("druid");
    Assert.assertEquals(null, dbUser);
  }

  @Test
  public void testDeleteNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    connector.deleteUser("druid");
  }

  @Test
  public void testCreateDuplicateUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] already exists.");
    connector.createUser("druid");
    connector.createUser("druid");
  }

  // role tests
  @Test
  public void testCreateRole() throws Exception
  {
    connector.createRole("druid");
    Map<String, Object> expectedRole = ImmutableMap.of(
        "name", "druid"
    );
    Map<String, Object> dbRole = connector.getRole("druid");
    Assert.assertEquals(expectedRole, dbRole);
  }

  @Test
  public void testDeleteNonExistentRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druid] does not exist.");
    connector.deleteRole("druid");
  }

  @Test
  public void testCreateDuplicateRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druid] already exists.");
    connector.createRole("druid");
    connector.createRole("druid");
  }

  // role and user tests
  @Test
  public void testAddAndRemoveRole() throws Exception
  {
    connector.createUser("druid");
    connector.createRole("druidRole");
    connector.assignRole("druid", "druidRole");

    List<Map<String, Object>> expectedUsersWithRole = ImmutableList.of(
        ImmutableMap.of("name", "druid")
    );

    List<Map<String, Object>> expectedRolesForUser = ImmutableList.of(
        ImmutableMap.of("name", "druidRole")
    );

    List<Map<String, Object>> usersWithRole = connector.getUsersWithRole("druidRole");
    List<Map<String, Object>> rolesForUser = connector.getRolesForUser("druid");

    Assert.assertEquals(expectedUsersWithRole, usersWithRole);
    Assert.assertEquals(expectedRolesForUser, rolesForUser);

    connector.unassignRole("druid", "druidRole");
    usersWithRole = connector.getUsersWithRole("druidRole");
    rolesForUser = connector.getRolesForUser("druid");

    Assert.assertEquals(ImmutableList.of(), usersWithRole);
    Assert.assertEquals(ImmutableList.of(), rolesForUser);
  }

  @Test
  public void testAddRoleToNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [nonUser] does not exist.");
    connector.createRole("druid");
    connector.assignRole("nonUser", "druid");
  }

  @Test
  public void testAddNonexistentRoleToUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [nonRole] does not exist.");
    connector.createUser("druid");
    connector.assignRole("druid", "nonRole");
  }

  @Test
  public void testAddExistingRoleToUserFails() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] already has role [druidRole].");
    connector.createUser("druid");
    connector.createRole("druidRole");
    connector.assignRole("druid", "druidRole");
    connector.assignRole("druid", "druidRole");
  }

  @Test
  public void testUnassignInvalidRoleAssignmentFails() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not have role [druidRole].");

    connector.createUser("druid");
    connector.createRole("druidRole");

    List<Map<String, Object>> usersWithRole = connector.getUsersWithRole("druidRole");
    List<Map<String, Object>> rolesForUser = connector.getRolesForUser("druid");

    Assert.assertEquals(ImmutableList.of(), usersWithRole);
    Assert.assertEquals(ImmutableList.of(), rolesForUser);

    connector.unassignRole("druid", "druidRole");
  }

  // role and permission tests
  @Test
  public void testAddPermissionToRole() throws Exception
  {
    connector.createUser("druid");
    connector.createRole("druidRole");
    connector.assignRole("druid", "druidRole");

    ResourceAction permission = new ResourceAction(
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    connector.addPermission("druidRole", permission);

    List<Map<String, Object>> expectedPerms = ImmutableList.of(
        ImmutableMap.of(
            "id", 1,
            "resourceAction", permission
        )
    );
    List<Map<String, Object>> dbPermsRole = connector.getPermissionsForRole("druidRole");
    Assert.assertEquals(expectedPerms, dbPermsRole);
    List<Map<String, Object>> dbPermsUser = connector.getPermissionsForUser("druid");
    Assert.assertEquals(expectedPerms, dbPermsUser);

    connector.deletePermission(1);
    dbPermsRole = connector.getPermissionsForRole("druidRole");
    Assert.assertEquals(ImmutableList.of(), dbPermsRole);
    dbPermsUser = connector.getPermissionsForUser("druid");
    Assert.assertEquals(ImmutableList.of(), dbPermsUser);
  }

  @Test
  public void testAddPermissionToNonExistentRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druidRole] does not exist.");

    ResourceAction permission = new ResourceAction(
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    connector.addPermission("druidRole", permission);
  }

  @Test
  public void testGetPermissionForNonExistentRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druidRole] does not exist.");
    connector.getPermissionsForRole("druidRole");
  }

  @Test
  public void testGetPermissionForNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    connector.getPermissionsForUser("druid");
  }

  // user credentials
  @Test
  public void testAddUserCredentials() throws Exception
  {
    char[] pass = "blah".toCharArray();
    connector.createUser("druid");
    connector.setUserCredentials("druid", pass);
    Assert.assertTrue(connector.checkCredentials("druid", pass));
    Assert.assertFalse(connector.checkCredentials("druid", "wrongPass".toCharArray()));

    Map<String, Object> creds = connector.getUserCredentials("druid");
    Assert.assertEquals("druid", creds.get("user_name"));
    byte[] salt = (byte[]) creds.get("salt");
    byte[] hash = (byte[]) creds.get("hash");
    int iterations = (Integer) creds.get("iterations");
    Assert.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assert.assertEquals(BasicAuthUtils.KEY_LENGTH / 8, hash.length);
    Assert.assertEquals(BasicAuthUtils.KEY_ITERATIONS, iterations);

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        pass,
        salt,
        iterations
    );
    Assert.assertArrayEquals(recalculatedHash, hash);
  }

  @Test
  public void testAddCredentialsToNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    char[] pass = "blah".toCharArray();
    connector.setUserCredentials("druid", pass);
  }

  @Test
  public void testGetCredentialsForNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    connector.getUserCredentials("druid");
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
