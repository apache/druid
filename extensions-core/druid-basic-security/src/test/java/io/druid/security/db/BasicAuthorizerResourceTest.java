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
import io.druid.security.basic.BasicAuthorizerResource;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.server.security.Action;
import io.druid.server.security.AllowAllAuthorizer;
import io.druid.server.security.AuthorizerMapper;
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

public class BasicAuthorizerResourceTest
{
  private static final String BASIC_AUTHORIZER_NAME = "basic";
  private static final String BASIC_AUTHORIZER_NAME2 = "basic2";

  private BasicAuthorizerResource resource;
  private HttpServletRequest req;
  private TestDerbyAuthorizerStorageConnector connector;

  @Rule
  public final TestDerbyAuthorizerStorageConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyAuthorizerStorageConnector.DerbyConnectorRule("test");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();


  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    connector = derbyConnectorRule.getConnector();

    AuthorizerMapper mapper = new AuthorizerMapper(
        ImmutableMap.of(
            BASIC_AUTHORIZER_NAME, new BasicRoleBasedAuthorizer(connector, BASIC_AUTHORIZER_NAME, 5000),
            BASIC_AUTHORIZER_NAME2, new BasicRoleBasedAuthorizer(connector, BASIC_AUTHORIZER_NAME2, 5000),
            "allowAll", new AllowAllAuthorizer()
        )
    );

    createAllTables();
    resource = new BasicAuthorizerResource(connector, mapper);
  }

  @After
  public void tearDown() throws Exception
  {
    dropAllTables();
  }

  @Test
  public void testSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid2");
    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid3");

    resource.createUser(req, BASIC_AUTHORIZER_NAME2, "druid4");
    resource.createUser(req, BASIC_AUTHORIZER_NAME2, "druid5");
    resource.createUser(req, BASIC_AUTHORIZER_NAME2, "druid6");

    List<Map<String, Object>> expectedUsers = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );
    List<Map<String, Object>> expectedUsers2 = ImmutableList.of(
        ImmutableMap.of("name", "druid4"),
        ImmutableMap.of("name", "druid5"),
        ImmutableMap.of("name", "druid6")
    );

    response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());

    response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers2, response.getEntity());
  }

  @Test
  public void testInvalidAuthorizer()
  {
    Response response = resource.getAllUsers(req, "invalidName");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        errorMapWithMsg("Basic authorizer with name [invalidName] does not exist."),
        response.getEntity()
    );
  }

  @Test
  public void testGetAllUsers()
  {
    Response response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid2");
    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid3");

    List<Map<String, Object>> expectedUsers = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );

    response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());
  }


  @Test
  public void testGetAllRoles()
  {
    Response response = resource.getAllRoles(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    resource.createRole(req, BASIC_AUTHORIZER_NAME, "druid");
    resource.createRole(req, BASIC_AUTHORIZER_NAME, "druid2");
    resource.createRole(req, BASIC_AUTHORIZER_NAME, "druid3");

    List<Map<String, Object>> expectedRoles = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );

    response = resource.getAllRoles(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoles, response.getEntity());
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.deleteUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testCreateDeleteRole()
  {
    Response response = resource.createRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.deleteRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [druidRole] does not exist."), response.getEntity());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [druidRole] does not exist."), response.getEntity());
  }

  @Test
  public void testRoleAssignment() throws Exception
  {
    Response response = resource.createRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(ImmutableMap.of("name", "druid")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.unassignRoleFromUser(req, BASIC_AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
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
    Response response = resource.createRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid2", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid2");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser2 = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid2"),
        "roles", ImmutableList.of(ImmutableMap.of("name", "druidRole")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser2, response.getEntity());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedRole = ImmutableMap.of(
        "role", ImmutableMap.of("name", "druidRole"),
        "users", ImmutableList.of(ImmutableMap.of("name", "druid"), ImmutableMap.of("name", "druid2")),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.deleteRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid"),
        "roles", ImmutableList.of(),
        "permissions", ImmutableList.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid2");
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
    Response response = resource.createRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    response = resource.addPermissionsToRole(req, BASIC_AUTHORIZER_NAME, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.addPermissionsToRole(req, BASIC_AUTHORIZER_NAME, "wrongRole", perms);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [wrongRole] does not exist."), response.getEntity());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
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

    response = resource.deletePermission(req, BASIC_AUTHORIZER_NAME, 7);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Permission with id [7] does not exist."), response.getEntity());

    response = resource.deletePermission(req, BASIC_AUTHORIZER_NAME, 2);
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
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
    Response response = resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createRole(req, BASIC_AUTHORIZER_NAME, "druidRole2");
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

    response = resource.addPermissionsToRole(req, BASIC_AUTHORIZER_NAME, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.addPermissionsToRole(req, BASIC_AUTHORIZER_NAME, "druidRole2", perms2);
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid2", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, BASIC_AUTHORIZER_NAME, "druid2", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
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

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid2");
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

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole");
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

    response = resource.getRole(req, BASIC_AUTHORIZER_NAME, "druidRole2");
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

    response = resource.deletePermission(req, BASIC_AUTHORIZER_NAME, 1);
    Assert.assertEquals(200, response.getStatus());

    response = resource.deletePermission(req, BASIC_AUTHORIZER_NAME, 4);
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
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

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid2");
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

    response = resource.unassignRoleFromUser(req, BASIC_AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromUser(req, BASIC_AUTHORIZER_NAME, "druid2", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid");
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

    response = resource.getUser(req, BASIC_AUTHORIZER_NAME, "druid2");
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
    connector.createUserTable(BASIC_AUTHORIZER_NAME);
    connector.createRoleTable(BASIC_AUTHORIZER_NAME);
    connector.createPermissionTable(BASIC_AUTHORIZER_NAME);
    connector.createUserRoleTable(BASIC_AUTHORIZER_NAME);

    connector.createUserTable(BASIC_AUTHORIZER_NAME2);
    connector.createRoleTable(BASIC_AUTHORIZER_NAME2);
    connector.createPermissionTable(BASIC_AUTHORIZER_NAME2);
    connector.createUserRoleTable(BASIC_AUTHORIZER_NAME2);
  }

  private void dropAllTables()
  {
    for (String table : connector.getTableNames(BASIC_AUTHORIZER_NAME)) {
      dropTable(table);
    }

    for (String table : connector.getTableNames(BASIC_AUTHORIZER_NAME2)) {
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
