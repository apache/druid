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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Provider;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.BasicSecurityAuthenticationException;
import org.apache.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;

@JsonTypeName("metadata")
public class MetadataStoreCredentialsValidator implements CredentialsValidator
{
  private static final Logger LOG = new Logger(MetadataStoreCredentialsValidator.class);
  private final Provider<BasicAuthenticatorCacheManager> cacheManager;
  private final PasswordHashGenerator hashGenerator = new PasswordHashGenerator();

  @JsonCreator
  public MetadataStoreCredentialsValidator(
      @JacksonInject Provider<BasicAuthenticatorCacheManager> cacheManager
  )
  {
    this.cacheManager = cacheManager;
  }

  @Override
  @Nullable
  public AuthenticationResult validateCredentials(
      String authenticatorName,
      String authorizerName,
      String username,
      char[] password
  )
  {
    Map<String, BasicAuthenticatorUser> userMap = cacheManager.get().getUserMap(authenticatorName);
    if (userMap == null) {
      throw new IAE("No userMap is available for authenticator with prefix: [%s]", authenticatorName);
    }

    BasicAuthenticatorUser user = userMap.get(username);
    if (user == null) {
      return null;
    }
    BasicAuthenticatorCredentials credentials = user.getCredentials();
    if (credentials == null) {
      return null;
    }

    byte[] recalculatedHash = hashGenerator.getOrComputePasswordHash(
        password,
        credentials.getSalt(),
        credentials.getIterations()
    );

    if (Arrays.equals(recalculatedHash, credentials.getHash())) {
      return new AuthenticationResult(username, authorizerName, authenticatorName, null);
    } else {
      LOG.debug("Password incorrect for metadata store user %s", username);
      throw new BasicSecurityAuthenticationException(Access.DEFAULT_ERROR_MESSAGE);
    }
  }
}
