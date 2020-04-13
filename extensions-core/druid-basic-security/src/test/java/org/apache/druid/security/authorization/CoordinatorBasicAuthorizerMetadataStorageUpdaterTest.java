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
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatorBasicAuthorizerMetadataStorageUpdaterTest
{
  private static final String AUTHORIZER_NAME = "test";

  private static final Map<String, BasicAuthorizerUser> BASE_USER_MAP = ImmutableMap.of(
      BasicAuthUtils.ADMIN_NAME,
      new BasicAuthorizerUser(BasicAuthUtils.ADMIN_NAME, ImmutableSet.of(BasicAuthUtils.ADMIN_NAME)),
      BasicAuthUtils.INTERNAL_USER_NAME,
      new BasicAuthorizerUser(BasicAuthUtils.INTERNAL_USER_NAME, ImmutableSet.of(
          BasicAuthUtils.INTERNAL_USER_NAME))
  );

  private static final Map<String, BasicAuthorizerRole> BASE_ROLE_MAP = ImmutableMap.of(
      BasicAuthUtils.ADMIN_NAME,
      new BasicAuthorizerRole(
          BasicAuthUtils.ADMIN_NAME,
          BasicAuthorizerPermission.makePermissionList(CoordinatorBasicAuthorizerMetadataStorageUpdater.SUPERUSER_PERMISSIONS)
      ),
      BasicAuthUtils.INTERNAL_USER_NAME,
      new BasicAuthorizerRole(
          BasicAuthUtils.INTERNAL_USER_NAME,
          BasicAuthorizerPermission.makePermissionList(CoordinatorBasicAuthorizerMetadataStorageUpdater.SUPERUSER_PERMISSIONS)
      )
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = new ObjectMapper(new SmileFactory());
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    updater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        new AuthorizerMapper(
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
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        objectMapper,
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    updater.start();
  }

  // user tests
  @Test
  public void testCreateDeleteUser()
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    Map<String, BasicAuthorizerUser> expectedUserMap = new HashMap<>(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of()));
    Map<String, BasicAuthorizerUser> actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);

    updater.deleteUser(AUTHORIZER_NAME, "druid");
    expectedUserMap.remove("druid");
    actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);
  }

  @Test
  public void testCreateDeleteGroupMapping()
  {
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    Map<String, BasicAuthorizerGroupMapping> expectedGroupMappingMap = new HashMap<>();
    expectedGroupMappingMap.put("druid", new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    Map<String, BasicAuthorizerGroupMapping> actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);

    updater.deleteGroupMapping(AUTHORIZER_NAME, "druid");
    expectedGroupMappingMap.remove("druid");
    actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
  }

  @Test
  public void testDeleteNonExistentUser()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    updater.deleteUser(AUTHORIZER_NAME, "druid");
  }

  @Test
  public void testDeleteNonExistentGroupMapping()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Group mapping [druid] does not exist.");
    updater.deleteGroupMapping(AUTHORIZER_NAME, "druid");
  }


  @Test
  public void testCreateDuplicateUser()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already exists.");
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createUser(AUTHORIZER_NAME, "druid");
  }

  @Test
  public void testCreateDuplicateGroupMapping()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Group mapping [druid] already exists.");
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
  }
  // role tests
  @Test
  public void testCreateDeleteRole()
  {
    updater.createRole(AUTHORIZER_NAME, "druid");
    Map<String, BasicAuthorizerRole> expectedRoleMap = new HashMap<>(BASE_ROLE_MAP);
    expectedRoleMap.put("druid", new BasicAuthorizerRole("druid", ImmutableList.of()));
    Map<String, BasicAuthorizerRole> actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedRoleMap, actualRoleMap);

    updater.deleteRole(AUTHORIZER_NAME, "druid");
    expectedRoleMap.remove("druid");
    actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testDeleteNonExistentRole()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [druid] does not exist.");
    updater.deleteRole(AUTHORIZER_NAME, "druid");
  }

  @Test
  public void testCreateDuplicateRole()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [druid] already exists.");
    updater.createRole(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druid");
  }

  // role, user, and group mapping tests
  @Test
  public void testAddAndRemoveRoleToUser()
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignUserRole(AUTHORIZER_NAME, "druid", "druidRole");

    Map<String, BasicAuthorizerUser> expectedUserMap = new HashMap<>(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of("druidRole")));

    Map<String, BasicAuthorizerRole> expectedRoleMap = new HashMap<>(BASE_ROLE_MAP);
    expectedRoleMap.put("druidRole", new BasicAuthorizerRole("druidRole", ImmutableList.of()));

    Map<String, BasicAuthorizerUser> actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );

    Map<String, BasicAuthorizerRole> actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedUserMap, actualUserMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);

    updater.unassignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of()));
    actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedUserMap, actualUserMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);
  }

  // role, user, and group mapping tests
  @Test
  public void testAddAndRemoveRoleToGroupMapping()
  {
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");

    Map<String, BasicAuthorizerGroupMapping> expectedGroupMappingMap = new HashMap<>();
    expectedGroupMappingMap.put("druid", new BasicAuthorizerGroupMapping("druid", "CN=test", ImmutableSet.of("druidRole")));

    Map<String, BasicAuthorizerRole> expectedRoleMap = new HashMap<>(BASE_ROLE_MAP);
    expectedRoleMap.put("druidRole", new BasicAuthorizerRole("druidRole", ImmutableList.of()));

    Map<String, BasicAuthorizerGroupMapping> actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );

    Map<String, BasicAuthorizerRole> actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);

    updater.unassignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
    expectedGroupMappingMap.put("druid", new BasicAuthorizerGroupMapping("druid", "CN=test", ImmutableSet.of()));
    actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testAddRoleToNonExistentUser()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [nonUser] does not exist.");
    updater.createRole(AUTHORIZER_NAME, "druid");
    updater.assignUserRole(AUTHORIZER_NAME, "nonUser", "druid");
  }

  @Test
  public void testAddRoleToNonExistentGroupMapping()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Group mapping [nonUser] does not exist.");
    updater.createRole(AUTHORIZER_NAME, "druid");
    updater.assignGroupMappingRole(AUTHORIZER_NAME, "nonUser", "druid");
  }

  @Test
  public void testAddNonexistentRoleToUser()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [nonRole] does not exist.");
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.assignUserRole(AUTHORIZER_NAME, "druid", "nonRole");
  }

  @Test
  public void testAddNonexistentRoleToGroupMapping()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [nonRole] does not exist.");
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "nonRole");
  }

  @Test
  public void testAddExistingRoleToUserFails()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already has role [druidRole].");
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
    updater.assignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
  }

  @Test
  public void testAddExistingRoleToGroupMappingFails()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Group mapping [druid] already has role [druidRole].");
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
    updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
  }

  @Test
  public void testAddExistingRoleToGroupMappingWithRoleFails()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Group mapping [druid] already has role [druidRole].");
    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", ImmutableSet.of("druidRole")));
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
  }

  @Test
  public void testUnassignInvalidRoleAssignmentToUserFails()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not have role [druidRole].");

    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");

    Map<String, BasicAuthorizerUser> expectedUserMap = new HashMap<>(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of()));

    Map<String, BasicAuthorizerRole> expectedRoleMap = new HashMap<>(BASE_ROLE_MAP);
    expectedRoleMap.put("druidRole", new BasicAuthorizerRole("druidRole", ImmutableList.of()));

    Map<String, BasicAuthorizerUser> actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );

    Map<String, BasicAuthorizerRole> actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedUserMap, actualUserMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);

    updater.unassignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
  }

  @Test
  public void testUnassignInvalidRoleAssignmentToGroupMappingFails()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Group mapping [druid] does not have role [druidRole].");


    updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    updater.createRole(AUTHORIZER_NAME, "druidRole");

    Map<String, BasicAuthorizerGroupMapping> expectedGroupMappingMap = new HashMap<>();
    expectedGroupMappingMap.put("druid", new BasicAuthorizerGroupMapping("druid", "CN=test", null));

    Map<String, BasicAuthorizerRole> expectedRoleMap = new HashMap<>(BASE_ROLE_MAP);
    expectedRoleMap.put("druidRole", new BasicAuthorizerRole("druidRole", ImmutableList.of()));

    Map<String, BasicAuthorizerGroupMapping> actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );

    Map<String, BasicAuthorizerRole> actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);

    updater.unassignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
  }


  // role and permission tests
  @Test
  public void testSetRolePermissions()
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignUserRole(AUTHORIZER_NAME, "druid", "druidRole");

    List<ResourceAction> permsToAdd = ImmutableList.of(
        new ResourceAction(
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.WRITE
        )
    );

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", permsToAdd);

    Map<String, BasicAuthorizerUser> expectedUserMap = new HashMap<>(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of("druidRole")));

    Map<String, BasicAuthorizerRole> expectedRoleMap = new HashMap<>(BASE_ROLE_MAP);
    expectedRoleMap.put(
        "druidRole",
        new BasicAuthorizerRole("druidRole", BasicAuthorizerPermission.makePermissionList(permsToAdd))
    );

    Map<String, BasicAuthorizerUser> actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );

    Map<String, BasicAuthorizerRole> actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedUserMap, actualUserMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", null);
    expectedRoleMap.put("druidRole", new BasicAuthorizerRole("druidRole", null));
    actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedUserMap, actualUserMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testAddPermissionToNonExistentRole()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [druidRole] does not exist.");

    List<ResourceAction> permsToAdd = ImmutableList.of(
        new ResourceAction(
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.WRITE
        )
    );

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", permsToAdd);
  }

  @Test
  public void testAddBadPermission()
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Invalid permission, resource name regex[??????????] does not compile.");
    updater.createRole(AUTHORIZER_NAME, "druidRole");

    List<ResourceAction> permsToAdd = ImmutableList.of(
        new ResourceAction(
            new Resource("??????????", ResourceType.DATASOURCE),
            Action.WRITE
        )
    );

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", permsToAdd);
  }
}
