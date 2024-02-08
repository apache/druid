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

package org.apache.druid.security.authorization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import org.apache.druid.security.basic.authorization.endpoint.BasicAuthorizerResource;
import org.apache.druid.security.basic.authorization.endpoint.CoordinatorBasicAuthorizerResourceHandler;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMappingFull;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRoleFull;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRoleSimplifiedPermissions;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUserFull;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUserFullSimplifiedPermissions;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@RunWith(MockitoJUnitRunner.class)
public class CoordinatorBasicAuthorizerResourceTest
{
  private static final String AUTHORIZER_NAME = "test";
  private static final String AUTHORIZER_NAME2 = "test2";
  private static final String AUTHORIZER_NAME3 = "test3";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  @Mock
  private AuthValidator authValidator;
  @Mock
  private HttpServletRequest req;
  @Mock
  private AuditManager auditManager;

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private BasicAuthorizerResource resource;
  private CoordinatorBasicAuthorizerMetadataStorageUpdater storageUpdater;

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    AuthorizerMapper authorizerMapper = new AuthorizerMapper(
        ImmutableMap.of(
            AUTHORIZER_NAME,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            AUTHORIZER_NAME2,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME2,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            AUTHORIZER_NAME3,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME3,
                null,
                null,
                "adminGroupMapping",
                null,
                null,
                null
            )
        )
    );

    storageUpdater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        authorizerMapper,
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    resource = new BasicAuthorizerResource(
        new CoordinatorBasicAuthorizerResourceHandler(
            storageUpdater,
            authorizerMapper,
            new ObjectMapper(new SmileFactory())
        ),
        authValidator,
        auditManager
    );

    storageUpdater.start();
  }

  @After
  public void tearDown()
  {
    storageUpdater.stop();
  }

  @Test
  public void testSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    response = resource.getAllUsers(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(),
        response.getEntity()
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(),
        response.getEntity()
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME3);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of("adminGroupMapping"),
        response.getEntity()
    );

    resource.createUser(req, AUTHORIZER_NAME, "druid");
    resource.createUser(req, AUTHORIZER_NAME, "druid2");
    resource.createUser(req, AUTHORIZER_NAME, "druid3");
    resource.createGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", new BasicAuthorizerGroupMapping("druidGroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", new BasicAuthorizerGroupMapping("druid2GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "druid3GroupMapping", new BasicAuthorizerGroupMapping("druid3GroupMapping", "", new HashSet<>()));

    resource.createUser(req, AUTHORIZER_NAME2, "druid4");
    resource.createUser(req, AUTHORIZER_NAME2, "druid5");
    resource.createUser(req, AUTHORIZER_NAME2, "druid6");
    resource.createGroupMapping(req, AUTHORIZER_NAME2, "druid4GroupMapping", new BasicAuthorizerGroupMapping("druid4GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME2, "druid5GroupMapping", new BasicAuthorizerGroupMapping("druid5GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME2, "druid6GroupMapping", new BasicAuthorizerGroupMapping("druid6GroupMapping", "", new HashSet<>()));

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid",
        "druid2",
        "druid3"
    );

    Set<String> expectedUsers2 = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid4",
        "druid5",
        "druid6"
    );

    response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());

    response = resource.getAllUsers(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers2, response.getEntity());

    Set<String> expectedGroupMappings = ImmutableSet.of(
        "druidGroupMapping",
        "druid2GroupMapping",
        "druid3GroupMapping"
    );

    Set<String> expectedGroupMappings2 = ImmutableSet.of(
        "druid4GroupMapping",
        "druid5GroupMapping",
        "druid6GroupMapping"
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappings, response.getEntity());

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappings2, response.getEntity());
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
    Response response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    resource.createUser(req, AUTHORIZER_NAME, "druid");
    resource.createUser(req, AUTHORIZER_NAME, "druid2");
    resource.createUser(req, AUTHORIZER_NAME, "druid3");

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid",
        "druid2",
        "druid3"
    );

    response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());
  }

  @Test
  public void testGetAllGroupMappings()
  {
    Response response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(),
        response.getEntity()
    );

    resource.createGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", new BasicAuthorizerGroupMapping("druidGroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", new BasicAuthorizerGroupMapping("druid2GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "druid3GroupMapping", new BasicAuthorizerGroupMapping("druid3GroupMapping", "", new HashSet<>()));

    Set<String> expectedGroupMappings = ImmutableSet.of(
        "druidGroupMapping",
        "druid2GroupMapping",
        "druid3GroupMapping"
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappings, response.getEntity());
  }

  @Test
  public void testGetAllRoles()
  {
    Response response = resource.getAllRoles(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    resource.createRole(req, AUTHORIZER_NAME, "druid");
    resource.createRole(req, AUTHORIZER_NAME, "druid2");
    resource.createRole(req, AUTHORIZER_NAME, "druid3");

    Set<String> expectedRoles = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid",
        "druid2",
        "druid3"
    );

    response = resource.getAllRoles(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoles, response.getEntity());
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = resource.createUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", null, null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerUser expectedUser = new BasicAuthorizerUser(
        "druid",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.deleteUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testCreateDeleteGroupMapping()
  {
    Response response = resource.createGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", new BasicAuthorizerGroupMapping("druidGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerGroupMapping expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "druidGroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.deleteGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Group mapping [druidGroupMapping] does not exist."), response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Group mapping [druidGroupMapping] does not exist."), response.getEntity());
  }

  @Test
  public void testCreateDeleteRole()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("druidRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.deleteRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [druidRole] does not exist."), response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [druidRole] does not exist."), response.getEntity());
  }

  @Test
  public void testUserRoleAssignment()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", null, null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerUser expectedUser = new BasicAuthorizerUser(
        "druid",
        ImmutableSet.of("druidRole")
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("druidRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedUser = new BasicAuthorizerUser(
        "druid",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testGroupMappingRoleAssignment()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", new BasicAuthorizerGroupMapping("druidGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerGroupMapping expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "druidGroupMapping",
        "", ImmutableSet.of("druidRole")
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("druidRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "druidGroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testDeleteAssignedRole()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "druid2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid2", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", new BasicAuthorizerGroupMapping("druidGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", new BasicAuthorizerGroupMapping("druid2GroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerUser expectedUser = new BasicAuthorizerUser(
        "druid",
        ImmutableSet.of("druidRole")
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerUser expectedUser2 = new BasicAuthorizerUser(
        "druid2",
        ImmutableSet.of("druidRole")
    );
    Assert.assertEquals(expectedUser2, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerGroupMapping expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "druidGroupMapping",
        "", ImmutableSet.of("druidRole")
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerGroupMapping expectedGroupMapping2 = new BasicAuthorizerGroupMapping(
        "druid2GroupMapping",
        "", ImmutableSet.of("druidRole")
    );
    Assert.assertEquals(expectedGroupMapping2, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("druidRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.deleteRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedUser = new BasicAuthorizerUser(
        "druid",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedUser2 = new BasicAuthorizerUser(
        "druid2",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser2, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "druidGroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    expectedGroupMapping2 = new BasicAuthorizerGroupMapping(
        "druid2GroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping2, response.getEntity());
  }

  @Test
  public void testRolesAndPerms()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "wrongRole", perms);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [wrongRole] does not exist."), response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("druidRole", BasicAuthorizerPermission.makePermissionList(perms));
    Assert.assertEquals(expectedRole, response.getEntity());

    List<ResourceAction> newPerms = ImmutableList.of(
        new ResourceAction(new Resource("D", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("F", ResourceType.CONFIG), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole", newPerms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedRole = new BasicAuthorizerRole("druidRole", BasicAuthorizerPermission.makePermissionList(newPerms));
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole", null);
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedRole = new BasicAuthorizerRole("druidRole", null);
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testUsersGroupMappingsRolesAndPerms()
  {
    Response response = resource.createUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "druid2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", new BasicAuthorizerGroupMapping("druidGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", new BasicAuthorizerGroupMapping("druid2GroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.createRole(req, AUTHORIZER_NAME, "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createRole(req, AUTHORIZER_NAME, "druidRole2");
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

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole2", perms2);
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid2", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid2", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("druidRole", BasicAuthorizerPermission.makePermissionList(perms));
    BasicAuthorizerRole expectedRole2 = new BasicAuthorizerRole("druidRole2", BasicAuthorizerPermission.makePermissionList(perms2));
    Set<BasicAuthorizerRole> expectedRoles = Sets.newHashSet(expectedRole, expectedRole2);

    BasicAuthorizerUserFull expectedUserFull = new BasicAuthorizerUserFull("druid", expectedRoles);
    response = resource.getUser(req, AUTHORIZER_NAME, "druid", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull, response.getEntity());
    BasicAuthorizerUserFullSimplifiedPermissions expectedUserFullSimplifiedPermissions =
        new BasicAuthorizerUserFullSimplifiedPermissions(
            "druid",
            BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
        );
    response = resource.getUser(req, AUTHORIZER_NAME, "druid", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions, response.getEntity());

    BasicAuthorizerUserFull expectedUserFull2 = new BasicAuthorizerUserFull("druid2", expectedRoles);
    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull2, response.getEntity());
    BasicAuthorizerUserFullSimplifiedPermissions expectedUserFullSimplifiedPermissions2 =
        new BasicAuthorizerUserFullSimplifiedPermissions(
            "druid2",
            BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
        );
    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions2, response.getEntity());

    BasicAuthorizerGroupMappingFull expectedGroupMappingFull = new BasicAuthorizerGroupMappingFull("druidGroupMapping", "", expectedRoles);
    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappingFull, response.getEntity());

    BasicAuthorizerGroupMappingFull expectedGroupMappingFull2 = new BasicAuthorizerGroupMappingFull("druid2GroupMapping", "", expectedRoles);
    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappingFull2, response.getEntity());

    Set<String> expectedUserSet = Sets.newHashSet("druid", "druid2");
    Set<String> expectedGroupMappingSet = Sets.newHashSet("druidGroupMapping", "druid2GroupMapping");
    BasicAuthorizerRoleFull expectedRoleFull = new BasicAuthorizerRoleFull(
        "druidRole",
        expectedUserSet,
        expectedGroupMappingSet,
        BasicAuthorizerPermission.makePermissionList(perms)
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull, response.getEntity());
    BasicAuthorizerRoleSimplifiedPermissions expectedRoleSimplifiedPerms = new BasicAuthorizerRoleSimplifiedPermissions(
        "druidRole",
        expectedUserSet,
        perms
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms, response.getEntity());
    expectedRoleSimplifiedPerms = new BasicAuthorizerRoleSimplifiedPermissions(
        "druidRole",
        null,
        perms
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", null, "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms, response.getEntity());

    BasicAuthorizerRoleFull expectedRoleFull2 = new BasicAuthorizerRoleFull(
        "druidRole2",
        expectedUserSet,
        expectedGroupMappingSet,
        BasicAuthorizerPermission.makePermissionList(perms2)
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull2, response.getEntity());
    BasicAuthorizerRoleSimplifiedPermissions expectedRoleSimplifiedPerms2 = new BasicAuthorizerRoleSimplifiedPermissions(
        "druidRole2",
        expectedUserSet,
        perms2
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms2, response.getEntity());
    expectedRoleSimplifiedPerms2 = new BasicAuthorizerRoleSimplifiedPermissions(
        "druidRole2",
        null,
        perms2
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole2", null, "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms2, response.getEntity());

    perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    perms2 = ImmutableList.of(
        new ResourceAction(new Resource("E", ResourceType.DATASOURCE), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "druidRole2", perms2);
    Assert.assertEquals(200, response.getStatus());

    expectedRole = new BasicAuthorizerRole("druidRole", BasicAuthorizerPermission.makePermissionList(perms));
    expectedRole2 = new BasicAuthorizerRole("druidRole2", BasicAuthorizerPermission.makePermissionList(perms2));
    expectedRoles = Sets.newHashSet(expectedRole, expectedRole2);
    expectedUserFull = new BasicAuthorizerUserFull("druid", expectedRoles);
    expectedUserFull2 = new BasicAuthorizerUserFull("druid2", expectedRoles);
    expectedUserFullSimplifiedPermissions = new BasicAuthorizerUserFullSimplifiedPermissions(
        "druid",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
    );
    expectedUserFullSimplifiedPermissions2 = new BasicAuthorizerUserFullSimplifiedPermissions(
        "druid2",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
    );

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "druid", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull2, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions2, response.getEntity());

    response = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "druid", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "druid2", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, "druidGroupMapping", "druidRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, "druid2GroupMapping", "druidRole2");
    Assert.assertEquals(200, response.getStatus());

    expectedUserFull = new BasicAuthorizerUserFull("druid", Sets.newHashSet(expectedRole2));
    expectedUserFull2 = new BasicAuthorizerUserFull("druid2", Sets.newHashSet(expectedRole));
    expectedRoleFull = new BasicAuthorizerRoleFull(
        "druidRole",
        Sets.newHashSet("druid2"),
        Sets.newHashSet("druid2GroupMapping"),
        BasicAuthorizerPermission.makePermissionList(perms)
    );
    expectedRoleFull2 = new BasicAuthorizerRoleFull(
        "druidRole2",
        Sets.newHashSet("druid"),
        Sets.newHashSet("druidGroupMapping"),
        BasicAuthorizerPermission.makePermissionList(perms2)
    );
    expectedUserFullSimplifiedPermissions = new BasicAuthorizerUserFullSimplifiedPermissions(
        "druid",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedUserFull.getRoles())
    );
    expectedUserFullSimplifiedPermissions2 = new BasicAuthorizerUserFullSimplifiedPermissions(
        "druid2",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedUserFull2.getRoles())
    );
    expectedRoleSimplifiedPerms = new BasicAuthorizerRoleSimplifiedPermissions(expectedRoleFull);
    expectedRoleSimplifiedPerms2 = new BasicAuthorizerRoleSimplifiedPermissions(expectedRoleFull2);

    response = resource.getUser(req, AUTHORIZER_NAME, "druid", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "druid", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull2, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "druid2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions2, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull, response.getEntity());
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull2, response.getEntity());
    response = resource.getRole(req, AUTHORIZER_NAME, "druidRole2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms2, response.getEntity());
  }

  @Test
  public void testConcurrentUpdate()
  {
    final int testMultiple = 100;

    // setup a user and the roles
    Response response = resource.createUser(req, AUTHORIZER_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    for (int i = 0; i < testMultiple; i++) {
      String roleName = "druidRole-" + i;
      response = resource.createRole(req, AUTHORIZER_NAME, roleName);
      Assert.assertEquals(200, response.getStatus());

      response = resource.setRolePermissions(req, AUTHORIZER_NAME, roleName, perms);
      Assert.assertEquals(200, response.getStatus());
    }

    ExecutorService exec = Execs.multiThreaded(testMultiple, "thread---");
    int[] responseCodesAssign = new int[testMultiple];

    // assign 'testMultiple' roles to the user concurrently
    List<Callable<Void>> addRoleCallables = new ArrayList<>();
    for (int i = 0; i < testMultiple; i++) {
      final int innerI = i;
      String roleName = "druidRole-" + i;
      addRoleCallables.add(
          () -> {
            Response response12 = resource.assignRoleToUser(req, AUTHORIZER_NAME, "druid", roleName);
            responseCodesAssign[innerI] = response12.getStatus();
            return null;
          }
      );
    }
    try {
      List<Future<Void>> futures = exec.invokeAll(addRoleCallables);
      for (Future future : futures) {
        future.get();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    // the API can return !200 if the update attempt fails by exhausting retries because of
    // too much contention from other conflicting requests, make sure that we don't get any successful requests
    // that didn't actually take effect
    Set<String> roleNames = getRoleNamesAssignedToUser("druid");
    for (int i = 0; i < testMultiple; i++) {
      String roleName = "druidRole-" + i;
      if (responseCodesAssign[i] == 200 && !roleNames.contains(roleName)) {
        Assert.fail(
            StringUtils.format("Got response status 200 for assigning role [%s] but user did not have role.", roleName)
        );
      }
    }

    // Now unassign the roles concurrently
    List<Callable<Void>> removeRoleCallables = new ArrayList<>();
    int[] responseCodesRemove = new int[testMultiple];

    for (int i = 0; i < testMultiple; i++) {
      final int innerI = i;
      String roleName = "druidRole-" + i;
      removeRoleCallables.add(
          () -> {
            Response response1 = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "druid", roleName);
            responseCodesRemove[innerI] = response1.getStatus();
            return null;
          }
      );
    }
    try {
      List<Future<Void>> futures = exec.invokeAll(removeRoleCallables);
      for (Future future : futures) {
        future.get();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    roleNames = getRoleNamesAssignedToUser("druid");
    for (int i = 0; i < testMultiple; i++) {
      String roleName = "druidRole-" + i;
      if (responseCodesRemove[i] == 200 && roleNames.contains(roleName)) {
        Assert.fail(
            StringUtils.format("Got response status 200 for removing role [%s] but user still has role.", roleName)
        );
      }
    }
  }

  private Set<String> getRoleNamesAssignedToUser(
      String user
  )
  {
    Response response = resource.getUser(req, AUTHORIZER_NAME, user, "", null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerUserFull userFull = (BasicAuthorizerUserFull) response.getEntity();
    Set<String> roleNames = new HashSet<>();
    for (BasicAuthorizerRole role : userFull.getRoles()) {
      roleNames.add(role.getName());
    }
    return roleNames;
  }

  private static Map<String, String> errorMapWithMsg(String errorMsg)
  {
    return ImmutableMap.of("error", errorMsg);
  }
}
