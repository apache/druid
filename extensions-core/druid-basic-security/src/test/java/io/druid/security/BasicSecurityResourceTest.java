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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityResource;
import io.druid.security.basic.db.SQLBasicSecurityStorageConnector;
import io.druid.security.db.TestDerbySecurityConnector;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

public class BasicSecurityResourceTest
{
  private BasicSecurityResource basicSecurityResource;
  private HttpServletRequest req;
  private TestDerbySecurityConnector connector;

  @Rule
  public final TestDerbySecurityConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbySecurityConnector.DerbyConnectorRule();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    connector = derbyConnectorRule.getConnector();
    createAllTables();
    basicSecurityResource = new BasicSecurityResource(connector);
  }

  @After
  public void tearDown() throws Exception
  {
    dropAllTables();
  }

  @Test
  public void testGetAllUsers()
  {
    Response response = basicSecurityResource.getAllUsers(req);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    basicSecurityResource.createUser(req, "druid");
    basicSecurityResource.createUser(req, "druid2");
    basicSecurityResource.createUser(req, "druid3");


    List<Map<String, Object>> expectedUsers = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );

    response = basicSecurityResource.getAllUsers(req);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());
  }

  @Test
  public void testGetAllRoles()
  {
    Response response = basicSecurityResource.getAllRoles(req);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    basicSecurityResource.createRole(req, "druid");
    basicSecurityResource.createRole(req, "druid2");
    basicSecurityResource.createRole(req, "druid3");

    List<Map<String, Object>> expectedRoles = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );

    response = basicSecurityResource.getAllRoles(req);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoles, response.getEntity());
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = basicSecurityResource.createUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.deleteUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.deleteUser(req, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testUserCredentials()
  {
    Response response = basicSecurityResource.createUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.updateUserCredentials(req, "druid", "helloworld");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUserCredentials(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> ent = (Map<String, Object> ) response.getEntity();
    Assert.assertEquals("druid", ent.get("user_name"));
    byte[] salt = (byte[]) ent.get("salt");
    byte[] hash = (byte[]) ent.get("hash");
    int iterations = (Integer) ent.get("iterations");
    Assert.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assert.assertEquals(BasicAuthUtils.KEY_LENGTH / 8, hash.length);
    Assert.assertEquals(BasicAuthUtils.KEY_ITERATIONS, iterations);

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        "helloworld".toCharArray(),
        salt,
        iterations
    );
    Assert.assertArrayEquals(recalculatedHash, hash);

    response = basicSecurityResource.deleteUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUserCredentials(req, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = basicSecurityResource.updateUserCredentials(req, "druid", "helloworld");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testCreateDeleteRole()
  {
    Response response = basicSecurityResource.createRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = basicSecurityResource.deleteRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.deleteRole(req, "druidRole");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [druidRole] does not exist."), response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [druidRole] does not exist."), response.getEntity());
  }

  @Test
  public void testRoleAssignment() throws Exception
  {
    Response response = basicSecurityResource.createRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.createUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(ImmutableMap.of("name", "druid")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = basicSecurityResource.unassignRoleFromUser(req, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testDeleteAssignedRole()
  {
    Response response = basicSecurityResource.createRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.createUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.createUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid2", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser2 = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid2"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser2, response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(ImmutableMap.of("name", "druid"), ImmutableMap.of("name", "druid2")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = basicSecurityResource.deleteRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());
    expectedUser2 = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid2"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser2, response.getEntity());
  }

  @Test
  public void testRolesAndPerms()
  {
    Response response = basicSecurityResource.createRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    response = basicSecurityResource.addPermissionsToRole(req, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.addPermissionsToRole(req, "wrongRole", perms);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [wrongRole] does not exist."), response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 1, "resourceAction", perms.get(0)),
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2))
        )
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = basicSecurityResource.deletePermission(req, 7);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Permission with id [7] does not exist."), response.getEntity());

    response = basicSecurityResource.deletePermission(req, 2);
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 1, "resourceAction", perms.get(0)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2))
        )
    );
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testUsersRolesAndPerms()
  {
    Response response = basicSecurityResource.createUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.createUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.createRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.createRole(req, "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    List<ResourceAction> perms2 = ImmutableList.of(
        new ResourceAction(new Resource("D", ResourceType.STATE), Action.READ),
        new ResourceAction(new Resource("E", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("F", ResourceType.CONFIG), Action.WRITE)
    );

    response = basicSecurityResource.addPermissionsToRole(req, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.addPermissionsToRole(req, "druidRole2", perms2);
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid2", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.assignRoleToUser(req, "druid2", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole"), ImmutableMap.of("name", "druidRole2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 1, "resourceAction", perms.get(0)),
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2)),
            ImmutableMap.of("id", 4, "resourceAction", perms2.get(0)),
            ImmutableMap.of("id", 5, "resourceAction", perms2.get(1)),
            ImmutableMap.of("id", 6, "resourceAction", perms2.get(2))
        )
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid2"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole"), ImmutableMap.of("name", "druidRole2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 1, "resourceAction", perms.get(0)),
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2)),
            ImmutableMap.of("id", 4, "resourceAction", perms2.get(0)),
            ImmutableMap.of("id", 5, "resourceAction", perms2.get(1)),
            ImmutableMap.of("id", 6, "resourceAction", perms2.get(2))
        )
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(ImmutableMap.of("name", "druid"), ImmutableMap.of("name", "druid2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 1, "resourceAction", perms.get(0)),
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2))
        )
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = basicSecurityResource.getRole(req, "druidRole2");
    Assert.assertEquals(200, response.getStatus());
    expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole2"),
        "users", ImmutableList.of(ImmutableMap.of("name", "druid"), ImmutableMap.of("name", "druid2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 4, "resourceAction", perms2.get(0)),
            ImmutableMap.of("id", 5, "resourceAction", perms2.get(1)),
            ImmutableMap.of("id", 6, "resourceAction", perms2.get(2))
        )
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = basicSecurityResource.deletePermission(req, 1);
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.deletePermission(req, 4);
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole"), ImmutableMap.of("name", "druidRole2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2)),
            ImmutableMap.of("id", 5, "resourceAction", perms2.get(1)),
            ImmutableMap.of("id", 6, "resourceAction", perms2.get(2))
        )
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid2"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole"), ImmutableMap.of("name", "druidRole2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2)),
            ImmutableMap.of("id", 5, "resourceAction", perms2.get(1)),
            ImmutableMap.of("id", 6, "resourceAction", perms2.get(2))
        )
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.unassignRoleFromUser(req, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.unassignRoleFromUser(req, "druid2", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = basicSecurityResource.getUser(req, "druid");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole2")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 5, "resourceAction", perms2.get(1)),
            ImmutableMap.of("id", 6, "resourceAction", perms2.get(2))
        )
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = basicSecurityResource.getUser(req, "druid2");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid2"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of(
            ImmutableMap.of("id", 2, "resourceAction", perms.get(1)),
            ImmutableMap.of("id", 3, "resourceAction", perms.get(2))
        )
    );
    Assert.assertEquals(expectedUser, response.getEntity());
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

  private static Map<String, String> errorMapWithMsg(String errorMsg)
  {
    return ImmutableMap.of("error", errorMsg);
  }
}
