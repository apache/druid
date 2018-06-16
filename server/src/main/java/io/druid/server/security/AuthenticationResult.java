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
import java.util.Objects;

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
   * Name of authenticator whom created the results
   *
   * If you found your self asking why the authenticatedBy field can be null please read this
   * https://github.com/druid-io/druid/pull/5706#discussion_r185940889
   */
  @Nullable
  private final String authenticatedBy;
  /**
   * parameter containing additional context information from an Authenticator
   */
  @Nullable
  private final Map<String, Object> context;

  public AuthenticationResult(
      final String identity,
      final String authorizerName,
      final String authenticatedBy,
      final Map<String, Object> context
  )
  {
    this.identity = identity;
    this.authorizerName = authorizerName;
    this.authenticatedBy = authenticatedBy;
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

  public String getAuthenticatedBy()
  {
    return authenticatedBy;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthenticationResult that = (AuthenticationResult) o;
    return Objects.equals(getIdentity(), that.getIdentity()) &&
           Objects.equals(getAuthorizerName(), that.getAuthorizerName()) &&
           Objects.equals(getAuthenticatedBy(), that.getAuthenticatedBy()) &&
           Objects.equals(getContext(), that.getContext());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getIdentity(), getAuthorizerName(), getAuthenticatedBy(), getContext());
  }
}
