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
import org.apache.druid.metadata.JUnit5TestDerbyConnector;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @RegisterExtension
  public static final JUnit5TestDerbyConnector DERBY_CONNECTOR_RULE = new JUnit5TestDerbyConnector();

  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp()
  {
    objectMapper = new ObjectMapper(new SmileFactory());
    TestDerbyConnector connector = DERBY_CONNECTOR_RULE.getConnector();
    MetadataStorageTablesConfig tablesConfig = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();
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
    Assertions.assertEquals(expectedUserMap, actualUserMap);

    updater.deleteUser(AUTHORIZER_NAME, "druid");
    expectedUserMap.remove("druid");
    actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );
    Assertions.assertEquals(expectedUserMap, actualUserMap);
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
    Assertions.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);

    updater.deleteGroupMapping(AUTHORIZER_NAME, "druid");
    expectedGroupMappingMap.remove("druid");
    actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );
    Assertions.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
  }

  @Test
  public void testDeleteNonExistentUser()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () ->
      updater.deleteUser(AUTHORIZER_NAME, "druid"));
    assertTrue(exception.getMessage().contains("User [druid] does not exist."));
  }

  @Test
  public void testDeleteNonExistentGroupMapping()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () ->
      updater.deleteGroupMapping(AUTHORIZER_NAME, "druid"));
    assertTrue(exception.getMessage().contains("Group mapping [druid] does not exist."));
  }


  @Test
  public void testCreateDuplicateUser()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createUser(AUTHORIZER_NAME, "druid");
      updater.createUser(AUTHORIZER_NAME, "druid");
    });
    assertTrue(exception.getMessage().contains("User [druid] already exists."));
  }

  @Test
  public void testCreateDuplicateGroupMapping()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
      updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
    });
    assertTrue(exception.getMessage().contains("Group mapping [druid] already exists."));
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
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);

    updater.deleteRole(AUTHORIZER_NAME, "druid");
    expectedRoleMap.remove("druid");
    actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testDeleteNonExistentRole()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () ->
      updater.deleteRole(AUTHORIZER_NAME, "druid"));
    assertTrue(exception.getMessage().contains("Role [druid] does not exist."));
  }

  @Test
  public void testCreateDuplicateRole()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createRole(AUTHORIZER_NAME, "druid");
      updater.createRole(AUTHORIZER_NAME, "druid");
    });
    assertTrue(exception.getMessage().contains("Role [druid] already exists."));
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

    Assertions.assertEquals(expectedUserMap, actualUserMap);
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);

    updater.unassignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of()));
    actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );

    Assertions.assertEquals(expectedUserMap, actualUserMap);
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);
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

    Assertions.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);

    updater.unassignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
    expectedGroupMappingMap.put("druid", new BasicAuthorizerGroupMapping("druid", "CN=test", ImmutableSet.of()));
    actualGroupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        updater.getCurrentGroupMappingMapBytes(AUTHORIZER_NAME)
    );

    Assertions.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testAddRoleToNonExistentUser()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createRole(AUTHORIZER_NAME, "druid");
      updater.assignUserRole(AUTHORIZER_NAME, "nonUser", "druid");
    });
    assertTrue(exception.getMessage().contains("User [nonUser] does not exist."));
  }

  @Test
  public void testAddRoleToNonExistentGroupMapping()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createRole(AUTHORIZER_NAME, "druid");
      updater.assignGroupMappingRole(AUTHORIZER_NAME, "nonUser", "druid");
    });
    assertTrue(exception.getMessage().contains("Group mapping [nonUser] does not exist."));
  }

  @Test
  public void testAddNonexistentRoleToUser()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createUser(AUTHORIZER_NAME, "druid");
      updater.assignUserRole(AUTHORIZER_NAME, "druid", "nonRole");
    });
    assertTrue(exception.getMessage().contains("Role [nonRole] does not exist."));
  }

  @Test
  public void testAddNonexistentRoleToGroupMapping()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
      updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "nonRole");
    });
    assertTrue(exception.getMessage().contains("Role [nonRole] does not exist."));
  }

  @Test
  public void testAddExistingRoleToUserFails()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createUser(AUTHORIZER_NAME, "druid");
      updater.createRole(AUTHORIZER_NAME, "druidRole");
      updater.assignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
      updater.assignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
    });
    assertTrue(exception.getMessage().contains("User [druid] already has role [druidRole]."));
  }

  @Test
  public void testAddExistingRoleToGroupMappingFails()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", null));
      updater.createRole(AUTHORIZER_NAME, "druidRole");
      updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
      updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
    });
    assertTrue(exception.getMessage().contains("Group mapping [druid] already has role [druidRole]."));
  }

  @Test
  public void testAddExistingRoleToGroupMappingWithRoleFails()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createGroupMapping(AUTHORIZER_NAME, new BasicAuthorizerGroupMapping("druid", "CN=test", ImmutableSet.of("druidRole")));
      updater.createRole(AUTHORIZER_NAME, "druidRole");
      updater.assignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
    });
    assertTrue(exception.getMessage().contains("Group mapping [druid] already has role [druidRole]."));
  }

  @Test
  public void testUnassignInvalidRoleAssignmentToUserFails()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {

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

      Assertions.assertEquals(expectedUserMap, actualUserMap);
      Assertions.assertEquals(expectedRoleMap, actualRoleMap);

      updater.unassignUserRole(AUTHORIZER_NAME, "druid", "druidRole");
    });
    assertTrue(exception.getMessage().contains("User [druid] does not have role [druidRole]."));
  }

  @Test
  public void testUnassignInvalidRoleAssignmentToGroupMappingFails()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {


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

      Assertions.assertEquals(expectedGroupMappingMap, actualGroupMappingMap);
      Assertions.assertEquals(expectedRoleMap, actualRoleMap);

      updater.unassignGroupMappingRole(AUTHORIZER_NAME, "druid", "druidRole");
    });
    assertTrue(exception.getMessage().contains("Group mapping [druid] does not have role [druidRole]."));
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

    Assertions.assertEquals(expectedUserMap, actualUserMap);
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", null);
    expectedRoleMap.put("druidRole", new BasicAuthorizerRole("druidRole", null));
    actualRoleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        updater.getCurrentRoleMapBytes(AUTHORIZER_NAME)
    );

    Assertions.assertEquals(expectedUserMap, actualUserMap);
    Assertions.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testAddPermissionToNonExistentRole()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {

      List<ResourceAction> permsToAdd = ImmutableList.of(
          new ResourceAction(
              new Resource("testResource", ResourceType.DATASOURCE),
              Action.WRITE
          )
      );

      updater.setPermissions(AUTHORIZER_NAME, "druidRole", permsToAdd);
    });
    assertTrue(exception.getMessage().contains("Role [druidRole] does not exist."));
  }

  @Test
  public void testAddBadPermission()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      updater.createRole(AUTHORIZER_NAME, "druidRole");

      List<ResourceAction> permsToAdd = ImmutableList.of(
          new ResourceAction(
              new Resource("??????????", ResourceType.DATASOURCE),
              Action.WRITE
          )
      );

      updater.setPermissions(AUTHORIZER_NAME, "druidRole", permsToAdd);
    });
    assertTrue(exception.getMessage().contains("Invalid permission, resource name regex[??????????] does not compile."));
  }
}
