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
import org.apache.commons.codec.Charsets;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.AuthorizerValidator;
import org.apache.druid.server.security.ResourceAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class BasicAuthorizerResourceTest
{
  private static final String AUTHORIZER_NAME = "AUTHORIZER_NAME";
  private static final String INVALID_AUTHORIZER_NAME = "INVALID_AUTHORIZER_NAME";
  private static final String USER_NAME = "USER_NAME";
  private static final String GROUP_MAPPING_NAME = "GROUP_MAPPING_NAME";
  private static final String ROLE_NAME = "ROLE_NAME";
  private static final byte[] SERIALIZED_ROLE_MAP = "SERIALIZED_ROLE_MAP".getBytes(Charsets.UTF_8);

  @Mock
  private BasicAuthorizerResourceHandler resourceHandler;
  @Mock
  private AuthorizerValidator authorizerValidator;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private BasicAuthorizerGroupMapping groupMapping;
  @Mock
  private ResourceAction resourceAction;
  private List<ResourceAction> resourceActions;
  @Mock
  private HttpServletRequest req;
  @Mock
  private Response allUsersRespose;
  @Mock
  private Response allGroupMappingsRespose;
  @Mock
  private Response userResponse;
  @Mock
  private Response groupMappingRespose;
  @Mock
  private Response createUserResponse;
  @Mock
  private Response deleteUserResponse;
  @Mock
  private Response createGroupMappingResponse;
  @Mock
  private Response deleteGroupMappingResponse;
  @Mock
  private Response roleResponse;
  @Mock
  private Response createRoleResponse;
  @Mock
  private Response deleteRoleResponse;
  @Mock
  private Response assignRoleToUserResponse;
  @Mock
  private Response unassignRoleFromUserResponse;
  @Mock
  private Response assignRoleToGroupMappingResponse;
  @Mock
  private Response unassignRoleFromGroupMappingResponse;
  @Mock
  private Response setRolePermissionsResponse;
  @Mock
  private Response getRolePermissionsResponse;
  @Mock
  private Response cachedSerializedUserMapResponse;
  @Mock
  private Response cachedSerializedGroupMapResponse;
  @Mock
  private Response authorizerUserUpdateResponse;
  @Mock
  private Response authorizerGroupMappingUpdateResponse;

  private BasicAuthorizerResource target;

  @Before
  public void setUp()
  {
    resourceActions = ImmutableList.of(resourceAction);
    Mockito.doThrow(IllegalArgumentException.class)
           .when(authorizerValidator)
           .validateAuthorizerName(INVALID_AUTHORIZER_NAME);
    Mockito.doReturn(allUsersRespose).when(resourceHandler).getAllUsers(AUTHORIZER_NAME);
    Mockito.doReturn(allGroupMappingsRespose).when(resourceHandler).getAllGroupMappings(AUTHORIZER_NAME);
    Mockito.doReturn(
        userResponse
    ).when(
        resourceHandler
    ).getUser(
        ArgumentMatchers.eq(AUTHORIZER_NAME),
        ArgumentMatchers.eq(USER_NAME),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.anyBoolean()
    );
    Mockito.doReturn(
        groupMappingRespose
    ).when(
        resourceHandler
    ).getGroupMapping(
        ArgumentMatchers.eq(AUTHORIZER_NAME),
        ArgumentMatchers.eq(GROUP_MAPPING_NAME),
        ArgumentMatchers.anyBoolean()
    );
    Mockito.doReturn(createUserResponse).when(resourceHandler).createUser(AUTHORIZER_NAME, USER_NAME);
    Mockito.doReturn(deleteUserResponse).when(resourceHandler).deleteUser(AUTHORIZER_NAME, USER_NAME);
    Mockito.doReturn(
        createGroupMappingResponse
    ).when(
        resourceHandler
    ).createGroupMapping(
        ArgumentMatchers.eq(AUTHORIZER_NAME),
        ArgumentMatchers.any(BasicAuthorizerGroupMapping.class)
    );
    Mockito.doReturn(deleteGroupMappingResponse)
           .when(resourceHandler)
           .deleteGroupMapping(AUTHORIZER_NAME, GROUP_MAPPING_NAME);
    Mockito.doReturn(
        roleResponse
    ).when(
        resourceHandler
    ).getRole(
        ArgumentMatchers.eq(AUTHORIZER_NAME),
        ArgumentMatchers.eq(ROLE_NAME),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.anyBoolean()
    );
    Mockito.doReturn(createRoleResponse).when(resourceHandler).createRole(AUTHORIZER_NAME, ROLE_NAME);
    Mockito.doReturn(deleteRoleResponse).when(resourceHandler).deleteRole(AUTHORIZER_NAME, ROLE_NAME);
    Mockito.doReturn(assignRoleToUserResponse)
           .when(resourceHandler)
           .assignRoleToUser(AUTHORIZER_NAME, USER_NAME, ROLE_NAME);
    Mockito.doReturn(unassignRoleFromUserResponse)
           .when(resourceHandler)
           .unassignRoleFromUser(AUTHORIZER_NAME, USER_NAME, ROLE_NAME);
    Mockito.doReturn(assignRoleToGroupMappingResponse)
           .when(resourceHandler)
           .assignRoleToGroupMapping(AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME);
    Mockito.doReturn(unassignRoleFromGroupMappingResponse)
           .when(resourceHandler)
           .unassignRoleFromGroupMapping(AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME);
    Mockito.doReturn(setRolePermissionsResponse)
           .when(resourceHandler)
           .setRolePermissions(AUTHORIZER_NAME, ROLE_NAME, resourceActions);
    Mockito.doReturn(getRolePermissionsResponse).when(resourceHandler).getRolePermissions(AUTHORIZER_NAME, ROLE_NAME);
    Mockito.doReturn(cachedSerializedUserMapResponse).when(resourceHandler).getCachedUserMaps(AUTHORIZER_NAME);
    Mockito.doReturn(cachedSerializedGroupMapResponse).when(resourceHandler).getCachedGroupMappingMaps(AUTHORIZER_NAME);
    Mockito.doReturn(authorizerUserUpdateResponse)
           .when(resourceHandler)
           .authorizerUserUpdateListener(AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
    Mockito.doReturn(authorizerGroupMappingUpdateResponse)
           .when(resourceHandler)
           .authorizerGroupMappingUpdateListener(AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);

    target = new BasicAuthorizerResource(resourceHandler, authorizerValidator);
  }

  @Test
  public void getAllUsersShouldReturnExpectedUsers()
  {
    Assert.assertEquals(allUsersRespose, target.getAllUsers(req, AUTHORIZER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getAllUsersWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getAllUsers(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void getAllGroupMappingsShouldReturnExpectedGroupMappings()
  {
    Assert.assertEquals(allGroupMappingsRespose, target.getAllGroupMappings(req, AUTHORIZER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getAllGroupMappingsWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getAllGroupMappings(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void getUserShouldReturnExpectedUser()
  {
    Assert.assertEquals(userResponse, target.getUser(req, AUTHORIZER_NAME, USER_NAME, null, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, null, null);
  }

  @Test
  public void getGroupMappingShouldReturnExpectedGroupMapping()
  {
    Assert.assertEquals(groupMappingRespose, target.getGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, null);
  }

  @Test
  public void createUserShouldReturnExpectedResponse()
  {
    Assert.assertEquals(createUserResponse, target.createUser(req, AUTHORIZER_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.createUser(req, INVALID_AUTHORIZER_NAME, USER_NAME);
  }

  @Test
  public void deleteUserShouldReturnExpectedResponse()
  {
    Assert.assertEquals(deleteUserResponse, target.deleteUser(req, AUTHORIZER_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.deleteUser(req, INVALID_AUTHORIZER_NAME, USER_NAME);
  }

  @Test
  public void createGroupMappingShouldReturnExpectedResponse()
  {
    Assert.assertEquals(
        createGroupMappingResponse,
        target.createGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, groupMapping)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void createGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.createGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, groupMapping);
  }

  @Test
  public void deleteGroupMappingShouldReturnExpectedResponse()
  {
    Assert.assertEquals(
        deleteGroupMappingResponse,
        target.deleteGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.deleteGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME);
  }

  @Test
  public void getRoleShouldReturnExpectedResult()
  {
    Assert.assertEquals(roleResponse, target.getRole(req, AUTHORIZER_NAME, ROLE_NAME, null, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME, null, null);
  }

  @Test
  public void createRoleShouldReturnExpectedResult()
  {
    Assert.assertEquals(createRoleResponse, target.createRole(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    target.createRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME);
  }

  @Test
  public void deleteRoleShouldReturnExpectedResult()
  {
    Assert.assertEquals(deleteRoleResponse, target.deleteRole(req, AUTHORIZER_NAME, ROLE_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteRoleWithInvalidAuthorizerNameShouldThrowException()
  {
    target.deleteRole(req, INVALID_AUTHORIZER_NAME, ROLE_NAME);
  }

  @Test
  public void assignRoleToUserShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        assignRoleToUserResponse,
        target.assignRoleToUser(req, AUTHORIZER_NAME, USER_NAME, ROLE_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignRoleToUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.assignRoleToUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, ROLE_NAME);
  }

  @Test
  public void unassignRoleFromUserShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        unassignRoleFromUserResponse,
        target.unassignRoleFromUser(req, AUTHORIZER_NAME, USER_NAME, ROLE_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void unassignRoleFromUserWithInvalidAuthorizerNameShouldThrowException()
  {
    target.unassignRoleFromUser(req, INVALID_AUTHORIZER_NAME, USER_NAME, ROLE_NAME);
  }

  @Test
  public void assignRoleToGroupMappingShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        assignRoleToGroupMappingResponse,
        target.assignRoleToGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void assignRoleToGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.assignRoleToGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME);
  }

  @Test
  public void unassignRoleFromGroupMappingShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        unassignRoleFromGroupMappingResponse,
        target.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void unassignRoleFromGroupMappingWithInvalidAuthorizerNameShouldThrowException()
  {
    target.unassignRoleFromGroupMapping(req, INVALID_AUTHORIZER_NAME, GROUP_MAPPING_NAME, ROLE_NAME);
  }

  @Test
  public void setRolePermissionsShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        setRolePermissionsResponse,
        target.setRolePermissions(req, AUTHORIZER_NAME, ROLE_NAME, resourceActions)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void setRolePermissionsWithInvalidAuthorizerNameShouldThrowException()
  {
    target.setRolePermissions(req, INVALID_AUTHORIZER_NAME, ROLE_NAME, resourceActions);
  }

  @Test
  public void getRolePermissionsShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        getRolePermissionsResponse,
        target.getRolePermissions(req, AUTHORIZER_NAME, ROLE_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void getRolePermissionsWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getRolePermissions(req, INVALID_AUTHORIZER_NAME, ROLE_NAME);
  }

  @Test
  public void getCachedSerializedUserMapShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        cachedSerializedUserMapResponse,
        target.getCachedSerializedUserMap(req, AUTHORIZER_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void getCachedSerializedUserMapWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getCachedSerializedUserMap(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void getCachedSerializedGroupMapShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        cachedSerializedGroupMapResponse,
        target.getCachedSerializedGroupMap(req, AUTHORIZER_NAME)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void getCachedSerializedGroupMapWithInvalidAuthorizerNameShouldThrowException()
  {
    target.getCachedSerializedGroupMap(req, INVALID_AUTHORIZER_NAME);
  }

  @Test
  public void authorizerUpdateListenerShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        authorizerUserUpdateResponse,
        target.authorizerUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void authorizerUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    target.authorizerUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
  }

  @Test
  public void authorizerUserUpdateListenerShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        authorizerUserUpdateResponse,
        target.authorizerUserUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void authorizerUserUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    target.authorizerUserUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
  }

  @Test
  public void authorizerGroupMappingUpdateListenerShouldReturnExpectedResult()
  {
    Assert.assertEquals(
        authorizerGroupMappingUpdateResponse,
        target.authorizerGroupMappingUpdateListener(req, AUTHORIZER_NAME, SERIALIZED_ROLE_MAP)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void authorizerGroupMappingUpdateListenerWithInvalidAuthorizerNameShouldThrowException()
  {
    target.authorizerGroupMappingUpdateListener(req, INVALID_AUTHORIZER_NAME, SERIALIZED_ROLE_MAP);
  }
}
