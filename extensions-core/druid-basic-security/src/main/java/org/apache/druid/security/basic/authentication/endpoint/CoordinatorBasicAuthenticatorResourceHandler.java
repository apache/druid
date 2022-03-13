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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.db.updater.BasicAuthenticatorMetadataStorageUpdater;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.server.initialization.jetty.HttpResponses;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class CoordinatorBasicAuthenticatorResourceHandler implements BasicAuthenticatorResourceHandler
{
  private final BasicAuthenticatorMetadataStorageUpdater storageUpdater;
  private final Map<String, BasicHTTPAuthenticator> authenticatorMap;
  private final ObjectMapper objectMapper;

  @Inject
  public CoordinatorBasicAuthenticatorResourceHandler(
      BasicAuthenticatorMetadataStorageUpdater storageUpdater,
      AuthenticatorMapper authenticatorMapper,
      @Smile ObjectMapper objectMapper
  )
  {
    this.storageUpdater = storageUpdater;
    this.objectMapper = objectMapper;

    this.authenticatorMap = new HashMap<>();
    for (Map.Entry<String, Authenticator> authenticatorEntry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      final String authenticatorName = authenticatorEntry.getKey();
      final Authenticator authenticator = authenticatorEntry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        authenticatorMap.put(
            authenticatorName,
            (BasicHTTPAuthenticator) authenticator
        );
      }
    }
  }

  @Override
  public Response getAllUsers(
      final String authenticatorName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authenticatorName)
    );

    return HttpResponses.OK.entity(userMap.keySet());
  }

  @Override
  public Response getUser(String authenticatorName, String userName)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authenticatorName)
    );

    try {
      BasicAuthenticatorUser user = userMap.get(userName);
      if (user == null) {
        throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
      }
      return HttpResponses.OK.entity(user);
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response createUser(String authenticatorName, String userName)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    try {
      storageUpdater.createUser(authenticatorName, userName);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response deleteUser(String authenticatorName, String userName)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    try {
      storageUpdater.deleteUser(authenticatorName, userName);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response updateUserCredentials(String authenticatorName, String userName, BasicAuthenticatorCredentialUpdate update)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    try {
      storageUpdater.setUserCredentials(authenticatorName, userName, update);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response getCachedSerializedUserMap(String authenticatorName)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    return HttpResponses.OK.entity(storageUpdater.getCachedSerializedUserMap(authenticatorName));
  }

  @Override
  public Response refreshAll()
  {
    storageUpdater.refreshAllNotification();
    return HttpResponses.OK.empty();
  }

  @Override
  public Response authenticatorUserUpdateListener(String authenticatorName, byte[] serializedUserMap)
  {
    return HttpResponses.NOT_FOUND.empty();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authenticatorMap.forEach(
        (authenticatorName, authenticator) ->
          loadStatus.put(authenticatorName, storageUpdater.getCachedUserMap(authenticatorName) != null)
    );
    return HttpResponses.OK.entity(loadStatus);
  }

  private static Response makeResponseForAuthenticatorNotFound(String authenticatorName)
  {
    return HttpResponses.BAD_REQUEST.error("Basic authenticator with name [%s] does not exist.", authenticatorName);
  }

  private static Response makeResponseForBasicSecurityDBResourceException(BasicSecurityDBResourceException bsre)
  {
    return HttpResponses.BAD_REQUEST.error(bsre.getMessage());
  }
}
