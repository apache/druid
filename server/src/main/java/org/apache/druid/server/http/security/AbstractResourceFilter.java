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

package org.apache.druid.server.http.security;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

public abstract class AbstractResourceFilter implements ResourceFilter, ContainerRequestFilter
{
  //https://jsr311.java.net/nonav/releases/1.1/spec/spec3.html#x3-520005
  @Context
  private HttpServletRequest req;

  private AuthorizerMapper authorizerMapper;

  @Inject
  public AbstractResourceFilter(
      AuthorizerMapper authorizerMapper
  )
  {
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public ContainerRequestFilter getRequestFilter()
  {
    return this;
  }

  @Override
  public ContainerResponseFilter getResponseFilter()
  {
    return null;
  }

  public HttpServletRequest getReq()
  {
    return req;
  }

  public AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }

  public void setAuthorizerMapper(AuthorizerMapper authorizerMapper)
  {
    this.authorizerMapper = authorizerMapper;
  }

  public AbstractResourceFilter setReq(HttpServletRequest req)
  {
    this.req = req;
    return this;
  }

  protected Action getAction(ContainerRequest request)
  {
    Action action;
    switch (request.getMethod()) {
      case "GET":
      case "HEAD":
        action = Action.READ;
        break;
      default:
        action = Action.WRITE;
    }
    return action;
  }
}
