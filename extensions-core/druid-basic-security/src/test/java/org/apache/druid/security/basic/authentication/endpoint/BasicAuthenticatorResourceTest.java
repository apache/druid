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

@RunWith(MockitoJUnitRunner.class)
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

  @Before
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
    Assert.assertNotNull(target.authenticatorUpdateListener(req, AUTHENTICATOR_NAME, SERIALIZED_USER_MAP));
  }

  @Test(expected = IllegalArgumentException.class)
  public void authenticatorUpdateListenerWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.authenticatorUpdateListener(req, INVALID_AUTHENTICATOR_NAME, SERIALIZED_USER_MAP);
  }

  @Test
  public void getCachedSerializedUserMapShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.getCachedSerializedUserMap(req, AUTHENTICATOR_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getCachedSerializedUserMapWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.getCachedSerializedUserMap(req, INVALID_AUTHENTICATOR_NAME);
  }

  @Test
  public void updateUserCredentialsShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.updateUserCredentials(req, AUTHENTICATOR_NAME, USER_NAME, update));
  }

  @Test(expected = IllegalArgumentException.class)
  public void updateUserCredentialsWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.updateUserCredentials(req, INVALID_AUTHENTICATOR_NAME, USER_NAME, update);
  }

  @Test
  public void deleteUserShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.deleteUser(req, AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteUserWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.deleteUser(req, INVALID_AUTHENTICATOR_NAME, USER_NAME);
  }

  @Test
  public void createUserShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.createUser(req, AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createUserWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.createUser(req, INVALID_AUTHENTICATOR_NAME, USER_NAME);
  }

  @Test
  public void getUserShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.getUser(req, AUTHENTICATOR_NAME, USER_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getUserWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.getUser(req, INVALID_AUTHENTICATOR_NAME, USER_NAME);
  }

  @Test
  public void getAllUsersShouldReturnExpectedResponse()
  {
    Assert.assertNotNull(target.getAllUsers(req, AUTHENTICATOR_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getAllUsersWithInvalidAuthenticatorNameShouldReturnExpectedResponse()
  {
    target.getAllUsers(req, INVALID_AUTHENTICATOR_NAME);
  }
}
