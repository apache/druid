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

package org.apache.druid.indexing.overlord.http.security;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.druid.server.security.AuthorizerValidation;

import javax.inject.Inject;
import java.util.function.Consumer;

public class AuthorizerResourceFilter implements ResourceFilter, ContainerRequestFilter
{
  private final Consumer<String> authorizerNameValidator;

  @Inject
  AuthorizerResourceFilter(@AuthorizerValidation Consumer<String> authorizerNameValidator)
  {
    this.authorizerNameValidator = authorizerNameValidator;
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

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    String authorizerName = Preconditions.checkNotNull(
        request.getPathSegments()
               .get(
                   Iterables.indexOf(
                       request.getPathSegments(),
                       input -> "authorizerName".equals(input.getPath())
                   ) + 1
               ).getPath()
    );
    authorizerNameValidator.accept(authorizerName);
    return request;
  }
}
