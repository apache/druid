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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
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

  @BeforeEach
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
    Assertions.assertNotNull(target.getAllUsers(req, AUTHORIZER_NAME));
  }

  @Test
  public void getAllUsersWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getAllUsers(req, INVALID_AUTHORIZER_NAME));
  }

  @Test
  public void getAllGroupMappingsShouldReturnExpectedGroupMappings()
  {
    Assertions.assertNotNull(target.getAllGroupMappings(req, AUTHORIZER_NAME));
  }

  @Test
  public void getAllGroupMappingsWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getAllGroupMappings(req, INVALID_AUTHORIZER_NAME));
  }

  @Test
  public void getUserShouldReturnExpectedUser()
  {
    Assertions.assertNotNull(target.getUser(req, AUTHORIZER_NAME, USER_NAME, null, null));
  }

  @Test
  public void getUserWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, null, null));
  }

  @Test
  public void getGroupMappingShouldReturnExpectedGroupMapping()
  {
    Assertions.assertNotNull(target.getGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, null));
  }

  @Test
  public void getGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, null));
  }

  @Test
  public void createUserShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.createUser(req, AUTHORIZER_NAME, USER_NAME));
  }

  @Test
  public void createUserWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.createUser(req, INVALID_AUTHORIZER_NAME, USER_NAME));
  }

  @Test
  public void deleteUserShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.deleteUser(req, AUTHORIZER_NAME, USER_NAME));
  }

  @Test
  public void deleteUserWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.deleteUser(req, INVALID_AUTHORIZER_NAME, USER_NAME));
  }

  @Test
  public void createGroupMappingShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.createGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, groupMapping));
  }

  @Test
  public void createGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.createGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, groupMapping));
  }

  @Test
  public void deleteGroupMappingShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.deleteGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME));
  }

  @Test
  public void deleteGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.deleteGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME));
  }

  @Test
  public void getRoleShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.getRole(req, AUTHORIZER_NAME, ROLE_NAME, null, null));
  }

  @Test
  public void getRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME, null, null));
  }

  @Test
  public void createRoleShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.createRole(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test
  public void createRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.createRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test
  public void deleteRoleShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.deleteRole(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test
  public void deleteRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.deleteRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test
  public void assignRoleToUserShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.assignRoleToUser(req, AUTHORIZER_NAME, USER_NAME, ROLE_NAME));
  }

  @Test
  public void assignRoleToUserWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.assignRoleToUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, ROLE_NAME));
  }

  @Test
  public void unassignRoleFromUserShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.unassignRoleFromUser(req, AUTHORIZER_NAME, USER_NAME, ROLE_NAME));
  }

  @Test
  public void unassignRoleFromUserWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.unassignRoleFromUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, ROLE_NAME));
  }

  @Test
  public void assignRoleToGroupMappingShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.assignRoleToGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME));
  }

  @Test
  public void assignRoleToGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.assignRoleToGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME));
  }

  @Test
  public void unassignRoleFromGroupMappingShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME));
  }

  @Test
  public void unassignRoleFromGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.unassignRoleFromGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME));
  }

  @Test
  public void setRolePermissionsShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.setRolePermissions(req, AUTHORIZER_NAME, ROLE_NAME, resourceActions));
  }

  @Test
  public void setRolePermissionsWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.setRolePermissions(req, INVALID_AUTHORIZER_NAME, ROLE_NAME, resourceActions));
  }

  @Test
  public void getRolePermissionsShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.getRolePermissions(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test
  public void getRolePermissionsWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getRolePermissions(req, INVALID_AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test
  public void getCachedSerializedUserMapShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.getCachedSerializedUserMap(req, AUTHORIZER_NAME));
  }

  @Test
  public void getCachedSerializedUserMapWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getCachedSerializedUserMap(req, INVALID_AUTHORIZER_NAME));
  }

  @Test
  public void getCachedSerializedGroupMapShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.getCachedSerializedGroupMap(req, AUTHORIZER_NAME));
  }

  @Test
  public void getCachedSerializedGroupMapWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getCachedSerializedGroupMap(req, INVALID_AUTHORIZER_NAME));
  }

  @Test
  public void authorizerUpdateListenerShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.authorizerUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test
  public void authorizerUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.authorizerUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test
  public void authorizerUserUpdateListenerShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.authorizerUserUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test
  public void authorizerUserUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.authorizerUserUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test
  public void authorizerGroupMappingUpdateListenerShouldReturnExpectedResult()
  {
    Assertions.assertNotNull(target.authorizerGroupMappingUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }

  @Test
  public void authorizerGroupMappingUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.authorizerGroupMappingUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP));
  }
}
