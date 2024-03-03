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

package org.apache.druid.testsEx.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.testing.utils.HttpUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;

/**
 * Coordinator client which performs various basic auth operations.
 */
public class BasicAuthHelper
{
  private final HttpClient adminClient;
  private final String coordinatorUrl;
  private final ObjectMapper jsonMapper;

  public BasicAuthHelper(HttpClient adminClient, String coordinatorUrl, ObjectMapper jsonMapper)
  {
    this.adminClient = adminClient;
    this.coordinatorUrl = coordinatorUrl;
    this.jsonMapper = jsonMapper;
  }

  public void createAuthenticationUser(String user)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/db/basic/users/%s",
            coordinatorUrl,
            user
        ),
        null
    );
  }

  public void createAuthorizationUser(String user)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s",
            coordinatorUrl,
            user
        ),
        null
    );
  }

  public void setPassword(String user, String password)
  {
    try {
      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          StringUtils.format(
              "%s/druid-ext/basic-security/authentication/db/basic/users/%s/credentials",
              coordinatorUrl,
              user
          ),
          jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate(password, 5000))
      );
    }
    catch (JsonProcessingException e) {
      throw new RE(e, "Failed to format credentials");
    }
  }

  public void createRole(String role)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s",
            coordinatorUrl,
            role
        ),
        null
    );
  }

  public void assignRoleToUser(String role, String user)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s/roles/%s",
            coordinatorUrl,
            user,
            role
        ),
        null
    );
  }

  public void updateRolePermissions(String role, List<ResourceAction> permissions)
  {
    byte[] permissionsBytes;
    try {
      permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
    }
    catch (JsonProcessingException e) {
      throw new RE(e, "Failed to encode permissions");
    }
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s/permissions",
            coordinatorUrl,
            role
        ),
        permissionsBytes
    );
  }

  public void createUserAndRoleWithPermissions(
      String user,
      String password,
      String role,
      List<ResourceAction> permissions
  )
  {
    createAuthenticationUser(user);
    createAuthorizationUser(user);
    setPassword(user, password);
    createRole(role);
    assignRoleToUser(role, user);
    updateRolePermissions(role, permissions);
  }

  public void cleanupUsersAndRoles()
  {
    List<String> roles = getRoles();
    for (String role : roles) {
      if ("admin".equals(role) || "druid_system".equals(role)) {
        continue;
      }
      dropRole(role);
    }
    List<String> users = getAuthorizationUsers();
    for (String user : users) {
      if ("admin".equals(user) || "druid_system".equals(user)) {
        continue;
      }
      dropAuthorizationUser(user);
    }
    users = getAuthenticationUsers();
    for (String user : users) {
      if ("admin".equals(user) || "druid_system".equals(user)) {
        continue;
      }
      dropAuthenticationUser(user);
    }
  }

  public static final TypeReference<List<String>> TYPE_REFERENCE_STRING_LIST =
      new TypeReference<List<String>>()
      {
      };

  public List<String> getRoles()
  {
    StatusResponseHolder holder = HttpUtil.makeRequest(
        adminClient,
        HttpMethod.GET,
        coordinatorUrl + "/druid-ext/basic-security/authorization/db/basic/roles",
        null
    );
    String content = holder.getContent();
    try {
      return jsonMapper.readValue(content, TYPE_REFERENCE_STRING_LIST);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropRole(String role)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s",
            coordinatorUrl,
            role
        ),
        null
    );
  }

  public List<String> getAuthenticationUsers()
  {
    StatusResponseHolder holder = HttpUtil.makeRequest(
        adminClient,
        HttpMethod.GET,
        coordinatorUrl + "/druid-ext/basic-security/authentication/db/basic/users",
        null
    );
    String content = holder.getContent();
    try {
      return jsonMapper.readValue(content, TYPE_REFERENCE_STRING_LIST);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropAuthenticationUser(String user)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/db/basic/users/%s",
            coordinatorUrl,
            user
        ),
        null
    );
  }

  public List<String> getAuthorizationUsers()
  {
    StatusResponseHolder holder = HttpUtil.makeRequest(
        adminClient,
        HttpMethod.GET,
        coordinatorUrl + "/druid-ext/basic-security/authorization/db/basic/users",
        null
    );
    String content = holder.getContent();
    try {
      return jsonMapper.readValue(content, TYPE_REFERENCE_STRING_LIST);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropAuthorizationUser(String user)
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s",
            coordinatorUrl,
            user
        ),
        null
    );
  }
}
