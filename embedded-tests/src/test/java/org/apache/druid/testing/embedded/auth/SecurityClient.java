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

package org.apache.druid.testing.embedded.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.testing.embedded.EmbeddedServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Client to call various basic auth APIs on the Coordinator. 
 */
public class SecurityClient
{
  private static final String AUTHENTICATOR_URL = "/druid-ext/basic-security/authentication/db/basic";
  private static final String AUTHORIZER_URL = "/druid-ext/basic-security/authorization/db/basic";

  private final EmbeddedServiceClient clients;

  SecurityClient(EmbeddedServiceClient clients)
  {
    this.clients = clients;
  }

  public void createAuthenticationUser(String username)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format(
            "%s/users/%s",
            AUTHENTICATOR_URL,
            StringUtils.urlEncode(username)
        )
    );
    sendRequest(mapper -> request);
  }

  public void deleteAuthenticationUser(String username)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/users/%s",
            AUTHENTICATOR_URL,
            StringUtils.urlEncode(username)
        )
    );
    sendRequest(mapper -> request);
  }

  public void setUserPassword(String username, String password)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format(
            "%s/users/%s/credentials",
            AUTHENTICATOR_URL,
            StringUtils.urlEncode(username)
        )
    );

    sendRequest(mapper -> request.jsonContent(mapper, Map.of("password", password)));
  }

  public void createAuthorizerUser(String username)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format(
            "%s/users/%s",
            AUTHORIZER_URL,
            StringUtils.urlEncode(username)
        )
    );
    sendRequest(mapper -> request);
  }

  public void deleteAuthorizerUser(String username)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/users/%s",
            AUTHORIZER_URL,
            StringUtils.urlEncode(username)
        )
    );
    sendRequest(mapper -> request);
  }

  public void createAuthorizerRole(String role)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format(
            "%s/roles/%s",
            AUTHORIZER_URL,
            StringUtils.urlEncode(role)
        )
    );
    sendRequest(mapper -> request);
  }

  public void deleteAuthorizerRole(String role)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/roles/%s",
            AUTHORIZER_URL,
            StringUtils.urlEncode(role)
        )
    );
    sendRequest(mapper -> request);
  }

  public void assignUserToRole(String user, String role)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format(
            "%s/users/%s/roles/%s",
            AUTHORIZER_URL,
            StringUtils.urlEncode(user),
            StringUtils.urlEncode(role)
        )
    );
    sendRequest(mapper -> request);
  }

  public void setPermissionsToRole(String role, List<ResourceAction> permissions)
  {
    final RequestBuilder request = new RequestBuilder(
        HttpMethod.POST,
        StringUtils.format(
            "%s/roles/%s/permissions/",
            AUTHORIZER_URL,
            StringUtils.urlEncode(role)
        )
    );
    sendRequest(mapper -> request.jsonContent(mapper, permissions));
  }

  private void sendRequest(Function<ObjectMapper, RequestBuilder> request)
  {
    clients.onLeaderCoordinator(request, null);
  }
}
