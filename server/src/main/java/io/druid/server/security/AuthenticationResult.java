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

package io.druid.server.security;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * An AuthenticationResult contains information about a successfully authenticated request.
 */
public class AuthenticationResult
{
  /**
   * the identity of the requester
   */
  private final String identity;

  /**
   * the name of the Authorizer that should handle the authenticated request.
   */
  private final String authorizerName;

  /**
   * parameter containing additional context information from an Authenticator
   */
  @Nullable
  private final Map<String, Object> context;

  public AuthenticationResult(
      final String identity,
      final String authorizerName,
      final Map<String, Object> context
  )
  {
    this.identity = identity;
    this.authorizerName = authorizerName;
    this.context = context;
  }

  public String getIdentity()
  {
    return identity;
  }

  public String getAuthorizerName()
  {
    return authorizerName;
  }

  public Map<String, Object> getContext()
  {
    return context;
  }
}
