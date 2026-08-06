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

package org.apache.druid.security.basic.authentication.endpoint;

import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.AuthValidator;
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

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BasicAuthenticatorResourceTest
{
  private static final String AUTHENTICATOR_NAME = "AUTHENTICATOR_NAME";
  private static final String INVALID_AUTHENTICATOR_NAME = "INVALID_AUTHENTICATOR_NAME";
  private static final String USER_NAME = "USER_NAME";
  private static final byte[] SERIALIZED_USER_MAP = "SERIALIZED_USER_MAP".getBytes(StandardCharsets.UTF_8);
  @Mock(answer = Answers.RETURNS_MOCKS)
  private BasicAuthenticatorResourceHandler handler;
  @Mock
  private AuthValidator authValidator;
  @Mock
  private HttpServletRequest req;
  @Mock
  private BasicAuthenticatorCredentialUpdate update;

  private BasicAuthenticatorResource target;

  @BeforeEach
  public void setUp()
  {
    Mockito.doThrow(IllegalArgumentException.class)
           .when(authValidator)
           .validateAuthenticatorName(INVALID_AUTHENTICATOR_NAME);

    target = new BasicAuthenticatorResource(handler, authValidator, null);
  }

  @Test
  public void authenticatorUpdateListenerShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.authenticatorUpdateListener(req, AUTHENTICATOR_NAME, SERIALIZED_USER_MAP));
  }

  @Test
  public void authenticatorUpdateListenerWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.authenticatorUpdateListener(req, INVALID_AUTHENTICATOR_NAME, SERIALIZED_USER_MAP));
  }

  @Test
  public void getCachedSerializedUserMapShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.getCachedSerializedUserMap(req, AUTHENTICATOR_NAME));
  }

  @Test
  public void getCachedSerializedUserMapWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getCachedSerializedUserMap(req, INVALID_AUTHENTICATOR_NAME));
  }

  @Test
  public void updateUserCredentialsShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.updateUserCredentials(req, AUTHENTICATOR_NAME, USER_NAME, update));
  }

  @Test
  public void updateUserCredentialsWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.updateUserCredentials(req, INVALID_AUTHENTICATOR_NAME, USER_NAME, update));
  }

  @Test
  public void deleteUserShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.deleteUser(req, AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test
  public void deleteUserWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.deleteUser(req, INVALID_AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test
  public void createUserShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.createUser(req, AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test
  public void createUserWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.createUser(req, INVALID_AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test
  public void getUserShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.getUser(req, AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test
  public void getUserWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getUser(req, INVALID_AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test
  public void getAllUsersShouldReturnExpectedResponse()
  {
    Assertions.assertNotNull(target.getAllUsers(req, AUTHENTICATOR_NAME));
  }

  @Test
  public void getAllUsersWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    assertThrows(IllegalArgumentException.class, () ->
      target.getAllUsers(req, INVALID_AUTHENTICATOR_NAME));
  }
}
