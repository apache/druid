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

package org.apache.druid.security.basic.authorization.endpoint;

import com.google.common.collect.ImmutableList;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.ResourceAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class BasicAuthorizerResourceTest
{
  private static final String AUTHORIZER_NAME = "AUTHORIZER_NAME";
  private static final String INVALID_AUTHORIZER_NAME = "INVALID_AUTHORIZER_NAME";
  private static final String USER_NAME = "USER_NAME";
  private static final String GROUP_MAPPING_NAME = "GROUP_MAPPING_NAME";
  private static final String ROLE_NAME = "ROLE_NAME";
  private static final byte[] SERIALIZED_ROLE_MAP = "SERIALIZED_ROLE_MAP".getBytes(StandardCharsets.UTF_8);

  @Mock(answer = Answers.RETURNS_MOCKS)
  private BasicAuthorizerResourceHandler resourceHandler;
  @Mock
  private AuthValidator authValidator;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private BasicAuthorizerGroupMapping groupMapping;
  @Mock
  private ResourceAction resourceAction;
  private List<ResourceAction> resourceActions;
  @Mock
  private HttpServletRequest req;

  private BasicAuthorizerResource target;

  @Before
  public void setUp()
  {
    resourceActions = ImmutableList.of(resourceAction);
    Mockito.doThrow(IllegalArgumentException.class)
           .when(authValidator)
           .validateAuthorizerName(INVALID_AUTHORIZER_NAME);

    target = new BasicAuthorizerResource(resourceHandler, authValidator, null);
  }

  @Test
  public void getAllUsersShouldReturnExpectedUsers()
  {
    Assert.assertNotNull(target.getAllUsers(req, AUTHORIZER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getAllUsersWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getAllUsers(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void getAllGroupMappingsShouldReturnExpectedGroupMappings()
  {
    Assert.assertNotNull(target.getAllGroupMappings(req, AUTHORIZER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getAllGroupMappingsWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getAllGroupMappings(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void getUserShouldReturnExpectedUser()
  {
    Assert.assertNotNull(target.getUser(req, AUTHORIZER_NAME, USER_NAME, null, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, null, null);
  }

  @Test
  public void getGroupMappingShouldReturnExpectedGroupMapping()
  {
    Assert.assertNotNull(target.getGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, null);
  }

  @Test
  public void createUserShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.createUser(req, AUTHORIZER_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.createUser(req, INVALID_AUTHORIZER_NAME, USER_NAME);
  }

  @Test
  public void deleteUserShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.deleteUser(req, AUTHORIZER_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.deleteUser(req, INVALID_AUTHORIZER_NAME, USER_NAME);
  }

  @Test
  public void createGroupMappingShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.createGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, groupMapping));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.createGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, groupMapping);
  }

  @Test
  public void deleteGroupMappingShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.deleteGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.deleteGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME);
  }

  @Test
  public void getRoleShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.getRole(req, AUTHORIZER_NAME, ROLE_NAME, null, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME, null, null);
  }

  @Test
  public void createRoleShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.createRole(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    target.createRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME);
  }

  @Test
  public void deleteRoleShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.deleteRole(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    target.deleteRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME);
  }

  @Test
  public void assignRoleToUserShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.assignRoleToUser(req, AUTHORIZER_NAME, USER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignRoleToUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.assignRoleToUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, ROLE_NAME);
  }

  @Test
  public void unassignRoleFromUserShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.unassignRoleFromUser(req, AUTHORIZER_NAME, USER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unassignRoleFromUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.unassignRoleFromUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, ROLE_NAME);
  }

  @Test
  public void assignRoleToGroupMappingShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.assignRoleToGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignRoleToGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.assignRoleToGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME);
  }

  @Test
  public void unassignRoleFromGroupMappingShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unassignRoleFromGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.unassignRoleFromGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME);
  }

  @Test
  public void setRolePermissionsShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.setRolePermissions(req, AUTHORIZER_NAME, ROLE_NAME, resourceActions));
  }

  @Test(expected = IllegalArgumentException.class)
  public void setRolePermissionsWithInvalidAuthorizerNameShouldThrowException()
  {
    target.setRolePermissions(req, INVALID_AUTHORIZER_NAME, ROLE_NAME, resourceActions);
  }

  @Test
  public void getRolePermissionsShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.getRolePermissions(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getRolePermissionsWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getRolePermissions(req, INVALID_AUTHORIZER_NAME, ROLE_NAME);
  }

  @Test
  public void getCachedSerializedUserMapShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.getCachedSerializedUserMap(req, AUTHORIZER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getCachedSerializedUserMapWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getCachedSerializedUserMap(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void getCachedSerializedGroupMapShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.getCachedSerializedGroupMap(req, AUTHORIZER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getCachedSerializedGroupMapWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getCachedSerializedGroupMap(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void authorizerUpdateListenerShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.authorizerUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test(expected = IllegalArgumentException.class)
  public void authorizerUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    target.authorizerUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
  }

  @Test
  public void authorizerUserUpdateListenerShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.authorizerUserUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test(expected = IllegalArgumentException.class)
  public void authorizerUserUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    target.authorizerUserUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
  }

  @Test
  public void authorizerGroupMappingUpdateListenerShouldReturnExpectedResult()
  {
    Assert.assertNotNull(target.authorizerGroupMappingUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test(expected = IllegalArgumentException.class)
  public void authorizerGroupMappingUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    target.authorizerGroupMappingUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
  }
}
