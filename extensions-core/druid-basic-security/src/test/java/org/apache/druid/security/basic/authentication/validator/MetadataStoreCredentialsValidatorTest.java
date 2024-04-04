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

package org.apache.druid.security.basic.authentication.validator;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import com.google.inject.util.Providers;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.security.basic.BasicSecurityAuthenticationException;
import org.apache.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MetadataStoreCredentialsValidatorTest
{
  private static final BasicAuthenticatorCredentials USER_A_CREDENTIALS = new BasicAuthenticatorCredentials(
      new BasicAuthenticatorCredentialUpdate("helloworld", 20)
  );

  private static final Provider<BasicAuthenticatorCacheManager> CACHE_MANAGER_PROVIDER = Providers.of(
      new BasicAuthenticatorCacheManager()
      {
        @Override
        public void handleAuthenticatorUserMapUpdate(String authenticatorPrefix, byte[] serializedUserMap)
        {

        }

        @Override
        public Map<String, BasicAuthenticatorUser> getUserMap(String authenticatorPrefix)
        {
          return ImmutableMap.of(
              "userA", new BasicAuthenticatorUser("userA", USER_A_CREDENTIALS),
              "userB", new BasicAuthenticatorUser("userB", null)
          );
        }
      }
  );

  private static final MetadataStoreCredentialsValidator VALIDATOR
      = new MetadataStoreCredentialsValidator(CACHE_MANAGER_PROVIDER);

  @Test
  public void validateBadAuthenticator()
  {
    String authenticatorName = "notbasic";
    String authorizerName = "basic";
    String username = "userA";
    String password = "helloworld";

    BasicAuthenticatorCacheManager cacheManager = EasyMock.createMock(BasicAuthenticatorCacheManager.class);
    EasyMock.expect(cacheManager.getUserMap(authenticatorName)).andReturn(null).times(1);
    EasyMock.replay(cacheManager);

    MetadataStoreCredentialsValidator validator = new MetadataStoreCredentialsValidator(Providers.of(cacheManager));

    IAE exception = Assert.assertThrows(
        IAE.class,
        () -> validator.validateCredentials(authenticatorName, authorizerName, username, password.toCharArray())
    );
    Assert.assertEquals("No userMap is available for authenticator with prefix: [notbasic]", exception.getMessage());

    EasyMock.verify(cacheManager);
  }

  @Test
  public void validateMissingCredentials()
  {
    String authenticatorName = "basic";
    String authorizerName = "basic";
    String username = "userB";
    String password = "helloworld";

    AuthenticationResult result = VALIDATOR.validateCredentials(authenticatorName, authorizerName, username, password.toCharArray());
    Assert.assertNull(result);
  }

  @Test
  public void validateMissingUser()
  {
    String authenticatorName = "basic";
    String authorizerName = "basic";
    String username = "userC";
    String password = "helloworld";

    AuthenticationResult result = VALIDATOR.validateCredentials(authenticatorName, authorizerName, username, password.toCharArray());
    Assert.assertNull(result);
  }

  @Test
  public void validateGoodCredentials()
  {
    String authenticatorName = "basic";
    String authorizerName = "basic";
    String username = "userA";
    String password = "helloworld";

    AuthenticationResult result = VALIDATOR.validateCredentials(authenticatorName, authorizerName, username, password.toCharArray());

    Assert.assertNotNull(result);
    Assert.assertEquals(username, result.getIdentity());
    Assert.assertEquals(authenticatorName, result.getAuthenticatedBy());
    Assert.assertEquals(authorizerName, result.getAuthorizerName());
    Assert.assertNull(result.getContext());
  }

  @Test
  public void validateBadCredentials()
  {
    String authenticatorName = "basic";
    String authorizerName = "basic";
    String username = "userA";
    String password = "badpassword";

    Exception exception = Assert.assertThrows(
        BasicSecurityAuthenticationException.class,
        () -> VALIDATOR.validateCredentials(authenticatorName, authorizerName, username, password.toCharArray())
    );
    Assert.assertEquals(Access.DEFAULT_ERROR_MESSAGE, exception.getMessage());
  }
}
