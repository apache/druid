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

package io.druid.security.authorization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.security.basic.BasicAuthCommonCacheConfig;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import io.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import io.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;
  private ObjectMapper objectMapper;

  @Before
  public void setUp() throws Exception
  {
    objectMapper = new ObjectMapper(new SmileFactory());
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    updater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        new AuthorizerMapper(
            ImmutableMap.of(
                AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    AUTHORIZER_NAME,
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
  public void testCreateDeleteUser() throws Exception
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    Map<String, BasicAuthorizerUser> expectedUserMap = Maps.newHashMap(BASE_USER_MAP);
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
  public void testDeleteNonExistentUser() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    updater.deleteUser(AUTHORIZER_NAME, "druid");
  }

  @Test
  public void testCreateDuplicateUser() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already exists.");
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createUser(AUTHORIZER_NAME, "druid");
  }

  // role tests
  @Test
  public void testCreateDeleteRole() throws Exception
  {
    updater.createRole(AUTHORIZER_NAME, "druid");
    Map<String, BasicAuthorizerRole> expectedRoleMap = Maps.newHashMap(BASE_ROLE_MAP);
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
  public void testDeleteNonExistentRole() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [druid] does not exist.");
    updater.deleteRole(AUTHORIZER_NAME, "druid");
  }

  @Test
  public void testCreateDuplicateRole() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [druid] already exists.");
    updater.createRole(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druid");
  }

  // role and user tests
  @Test
  public void testAddAndRemoveRole() throws Exception
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignRole(AUTHORIZER_NAME, "druid", "druidRole");

    Map<String, BasicAuthorizerUser> expectedUserMap = Maps.newHashMap(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of("druidRole")));

    Map<String, BasicAuthorizerRole> expectedRoleMap = Maps.newHashMap(BASE_ROLE_MAP);
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

    updater.unassignRole(AUTHORIZER_NAME, "druid", "druidRole");
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of()));
    actualUserMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );

    Assert.assertEquals(expectedUserMap, actualUserMap);
    Assert.assertEquals(expectedRoleMap, actualRoleMap);
  }

  @Test
  public void testAddRoleToNonExistentUser() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [nonUser] does not exist.");
    updater.createRole(AUTHORIZER_NAME, "druid");
    updater.assignRole(AUTHORIZER_NAME, "nonUser", "druid");
  }

  @Test
  public void testAddNonexistentRoleToUser() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("Role [nonRole] does not exist.");
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.assignRole(AUTHORIZER_NAME, "druid", "nonRole");
  }

  @Test
  public void testAddExistingRoleToUserFails() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already has role [druidRole].");
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignRole(AUTHORIZER_NAME, "druid", "druidRole");
    updater.assignRole(AUTHORIZER_NAME, "druid", "druidRole");
  }

  @Test
  public void testUnassignInvalidRoleAssignmentFails() throws Exception
  {
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not have role [druidRole].");

    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");

    Map<String, BasicAuthorizerUser> expectedUserMap = Maps.newHashMap(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of()));

    Map<String, BasicAuthorizerRole> expectedRoleMap = Maps.newHashMap(BASE_ROLE_MAP);
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

    updater.unassignRole(AUTHORIZER_NAME, "druid", "druidRole");
  }

  // role and permission tests
  @Test
  public void testSetRolePermissions() throws Exception
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignRole(AUTHORIZER_NAME, "druid", "druidRole");

    List<ResourceAction> permsToAdd = ImmutableList.of(
        new ResourceAction(
            new Resource("testResource", ResourceType.DATASOURCE),
            Action.WRITE
        )
    );

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", permsToAdd);

    Map<String, BasicAuthorizerUser> expectedUserMap = Maps.newHashMap(BASE_USER_MAP);
    expectedUserMap.put("druid", new BasicAuthorizerUser("druid", ImmutableSet.of("druidRole")));

    Map<String, BasicAuthorizerRole> expectedRoleMap = Maps.newHashMap(BASE_ROLE_MAP);
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
  public void testAddPermissionToNonExistentRole() throws Exception
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
  public void testAddBadPermission() throws Exception
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
