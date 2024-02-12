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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public class SecurityClient
{
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String coordinator;
  private final StatusResponseHandler responseHandler;
  // TODO: Inject this value
  private final String adminPassword;

  @Inject
  SecurityClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.coordinator = config.getCoordinatorUrl();
    this.responseHandler = StatusResponseHandler.getInstance();
    this.adminPassword = "priest";
  }

  public StatusResponseHolder createAuthenticationUser(String username) throws IOException
  {
    final Request request = new Request(
        HttpMethod.POST,
        new URL(
            StringUtils.format(
                "%s/users/%s",
                getAuthenticatorURL(),
                StringUtils.urlEncode(username)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder deleteAuthenticationUser(String username) throws IOException
  {
    final Request request = new Request(
        HttpMethod.DELETE,
        new URL(
            StringUtils.format(
                "%s/users/%s",
                getAuthenticatorURL(),
                StringUtils.urlEncode(username)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder setUserPassword(String username, String password) throws IOException
  {
    final Request request = new Request(
        HttpMethod.POST,
        new URL(
            StringUtils.format(
                "%s/users/%s/credentials",
                getAuthenticatorURL(),
                StringUtils.urlEncode(username)
            )
        )
    );

    request.setBasicAuthentication("admin", adminPassword)
           .setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(ImmutableMap.of("password", password)));
    return sendRequest(request);
  }

  public StatusResponseHolder createAuthorizerUser(String username) throws IOException
  {
    final Request request = new Request(
        HttpMethod.POST,
        new URL(
            StringUtils.format(
                "%s/users/%s",
                getAuthorizerURL(),
                StringUtils.urlEncode(username)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder deleteAuthorizerUser(String username) throws IOException
  {
    final Request request = new Request(
        HttpMethod.DELETE,
        new URL(
            StringUtils.format(
                "%s/users/%s",
                getAuthorizerURL(),
                StringUtils.urlEncode(username)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder createAuthorizerRole(String role) throws IOException
  {
    final Request request = new Request(
        HttpMethod.POST,
        new URL(
            StringUtils.format(
                "%s/roles/%s",
                getAuthorizerURL(),
                StringUtils.urlEncode(role)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder deleteAuthorizerRole(String role) throws IOException
  {
    final Request request = new Request(
        HttpMethod.DELETE,
        new URL(
            StringUtils.format(
                "%s/roles/%s",
                getAuthorizerURL(),
                StringUtils.urlEncode(role)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder assignUserToRole(String user, String role) throws IOException
  {
    final Request request = new Request(
        HttpMethod.POST,
        new URL(
            StringUtils.format(
                "%s/users/%s/roles/%s",
                getAuthorizerURL(),
                StringUtils.urlEncode(user),
                StringUtils.urlEncode(role)
            )
        )
    ).setBasicAuthentication("admin", adminPassword);
    return sendRequest(request);
  }

  public StatusResponseHolder setPermissionsToRole(String role, List<ResourceAction> permissions) throws IOException
  {
    final Request request = new Request(
        HttpMethod.POST,
        new URL(
            StringUtils.format(
                "%s/roles/%s/permissions/",
                getAuthorizerURL(),
                StringUtils.urlEncode(role)
            )
        )
    ).setBasicAuthentication("admin", adminPassword)
     .setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(permissions));
    return sendRequest(request);
  }

  private StatusResponseHolder sendRequest(Request request)
  {
    try {
      final StatusResponseHolder response = httpClient.go(
          request,
          responseHandler
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while creating users status [%s] content [%s]",
            response.getStatus(),
            response.getContent()
        );
      }

      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getAuthenticatorURL()
  {
    return StringUtils.format(
        "%s/druid-ext/basic-security/authentication/db/basic",
        coordinator
    );
  }

  private String getAuthorizerURL()
  {
    return StringUtils.format(
        "%s/druid-ext/basic-security/authorization/db/basic",
        coordinator
    );
  }
}
