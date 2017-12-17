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

package io.druid.security.basic.authentication.endpoint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class DefaultBasicAuthenticatorResourceHandler implements BasicAuthenticatorResourceHandler
{
  private static final Logger log = new Logger(DefaultBasicAuthenticatorResourceHandler.class);
  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  private final BasicAuthenticatorCacheManager cacheManager;
  private final Map<String, BasicHTTPAuthenticator> authenticatorMap;

  @Inject
  public DefaultBasicAuthenticatorResourceHandler(
      BasicAuthenticatorCacheManager cacheManager,
      AuthenticatorMapper authenticatorMapper
  )
  {
    this.cacheManager = cacheManager;

    this.authenticatorMap = Maps.newHashMap();
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
  public Response authenticatorUpdateListener(String authenticatorName, byte[] serializedUserMap)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      String errMsg = StringUtils.format("Received update for unknown authenticator[%s]", authenticatorName);
      log.error(errMsg);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format(errMsg)
                     ))
                     .build();
    }

    cacheManager.handleAuthenticatorUpdate(authenticatorName, serializedUserMap);
    return Response.ok().build();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authenticatorMap.forEach(
        (authenticatorName, authenticator) -> {
          loadStatus.put(authenticatorName, cacheManager.getUserMap(authenticatorName) != null);
        }
    );
    return Response.ok(loadStatus).build();
  }
}
