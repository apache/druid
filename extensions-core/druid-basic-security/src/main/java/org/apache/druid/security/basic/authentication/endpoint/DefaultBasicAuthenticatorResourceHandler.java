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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class DefaultBasicAuthenticatorResourceHandler implements BasicAuthenticatorResourceHandler
{
  private static final Logger log = new Logger(DefaultBasicAuthenticatorResourceHandler.class);
  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();
  private static final String UNKNOWN_AUTHENTICATOR_MSG_FORMAT = "Received user update for unknown authenticator[%s]";

  private final BasicAuthenticatorCacheManager cacheManager;
  private final Map<String, BasicHTTPAuthenticator> authenticatorMap;

  @Inject
  public DefaultBasicAuthenticatorResourceHandler(
      BasicAuthenticatorCacheManager cacheManager,
      AuthenticatorMapper authenticatorMapper
  )
  {
    this.cacheManager = cacheManager;

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
  public Response getAllUsers(String authenticatorName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getUser(String authenticatorName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createUser(String authenticatorName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteUser(String authenticatorName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response updateUserCredentials(
      String authenticatorName,
      String userName,
      BasicAuthenticatorCredentialUpdate update
  )
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedSerializedUserMap(String authenticatorName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response refreshAll()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response authenticatorUserUpdateListener(String authenticatorName, byte[] serializedUserMap)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      log.error(UNKNOWN_AUTHENTICATOR_MSG_FORMAT, authenticatorName);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format(UNKNOWN_AUTHENTICATOR_MSG_FORMAT, authenticatorName)
                     ))
                     .build();
    }

    cacheManager.handleAuthenticatorUserMapUpdate(authenticatorName, serializedUserMap);
    return Response.ok().build();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authenticatorMap.forEach((authenticatorName, authenticator) ->
                                 loadStatus.put(authenticatorName, cacheManager.getUserMap(authenticatorName) != null)
    );
    return Response.ok(loadStatus).build();
  }
}
