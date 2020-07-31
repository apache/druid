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

package org.apache.druid.server.security;

import org.apache.druid.common.utils.IdUtils;

/**
 * Utility functions to validate the an authorizer.
 */
public class AuthValidator
{
  private static final String AUTHORIZER_NAME = "authorizerName";
  private static final String AUTHENTICATOR_NAME = "authenticatorName";

  /**
   * Validates the provided authorizerName.
   *
   * @param authorizerName the name of the authorizer.
   * @throws IllegalArgumentException on invalid authorizer names.
   */
  public void validateAuthorizerName(String authorizerName)
  {
    IdUtils.validateId(AUTHORIZER_NAME, authorizerName);
  }

  /**
   * Validates the provided authenticatorName.
   *
   * @param authenticatorName the name of the authenticator.
   * @throws IllegalArgumentException on invalid authenticator names.
   */
  public void validateAuthenticatorName(String authenticatorName)
  {
    IdUtils.validateId(AUTHENTICATOR_NAME, authenticatorName);
  }
}
