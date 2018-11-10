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

import java.util.HashMap;
import java.util.Map;

public class AuthTestUtils
{
  public static final AuthenticatorMapper TEST_AUTHENTICATOR_MAPPER;
  public static final AuthorizerMapper TEST_AUTHORIZER_MAPPER;

  static {
    final Map<String, Authenticator> defaultMap = new HashMap<>();
    defaultMap.put(AuthConfig.ALLOW_ALL_NAME, new AllowAllAuthenticator());
    TEST_AUTHENTICATOR_MAPPER = new AuthenticatorMapper(defaultMap);

    TEST_AUTHORIZER_MAPPER = new AuthorizerMapper(null) {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new AllowAllAuthorizer();
      }
    };
  }
}
