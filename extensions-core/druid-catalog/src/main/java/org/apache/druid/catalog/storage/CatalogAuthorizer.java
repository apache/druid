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

package org.apache.druid.catalog.storage;

import org.apache.druid.catalog.model.SchemaRegistry.SchemaSpec;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

/**
 * Encapsulates the details of catalog authorization.
 */
public class CatalogAuthorizer
{
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public CatalogAuthorizer(
      AuthorizerMapper authorizerMapper)
  {
    this.authorizerMapper = authorizerMapper;
  }

  public AuthorizerMapper mapper()
  {
    return authorizerMapper;
  }

  public void authorizeTable(SchemaSpec schema, String name, Action action, HttpServletRequest request)
  {
    if (action == Action.WRITE && !schema.writable()) {
      throw new ForbiddenException(
          "Cannot create table definitions in schema: " + schema.name());
    }
    authorize(schema.securityResource(), name, action, request);
  }

  public void authorize(String resource, String key, Action action, HttpServletRequest request)
  {
    final Access authResult = authorizeAccess(resource, key, action, request);
    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }
  }

  public boolean isAuthorized(String resource, String key, Action action, HttpServletRequest request)
  {
    final Access authResult = authorizeAccess(resource, key, action, request);
    return authResult.isAllowed();
  }

  public Access authorizeAccess(String resource, String key, Action action, HttpServletRequest request)
  {
    return AuthorizationUtils.authorizeResourceAction(
        request,
        new ResourceAction(new Resource(key, resource), action),
        authorizerMapper
    );
  }

  public ResourceAction resourceAction(SchemaSpec schema, String name, Action action)
  {
    return new ResourceAction(new Resource(name, schema.securityResource()), action);
  }

  public Action inferAction(HttpServletRequest request)
  {
    switch (request.getMethod()) {
      case "GET":
      case "HEAD":
        return Action.READ;
      default:
        return Action.WRITE;
    }
  }
}
