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

import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import com.metamx.http.client.HttpClient;
import io.druid.java.util.common.ISE;

import java.util.List;

/**
 * Singleton utility object that creates escalated HttpClients using a configuration-specified Authenticator's
 * getEscalatedClient() method.
 */
public class AuthenticatorHttpClientWrapper
{
  private final AuthConfig authConfig;
  private Authenticator internalAuthenticator;

  public AuthenticatorHttpClientWrapper(
      final AuthConfig authConfig,
      final Injector injector
  )
  {
    this.authConfig = authConfig;

    if (authConfig.isEnabled()) {
      Preconditions.checkNotNull(
          authConfig.getInternalAuthenticator(),
          "Auth is enabled but no internal authenticator is configured."
      );
      Preconditions.checkNotNull(
          authConfig.getAuthenticatorChain(),
          "Auth is enabled but no authenticators have been configured."
      );

      List<Authenticator> authenticators = AuthenticationUtils.getAuthenticatorChainFromConfig(
          authConfig.getAuthenticatorChain(),
          injector
      );
      String internalAuthenticatorName = authConfig.getInternalAuthenticator();
      for (Authenticator authenticator : authenticators) {
        if (authenticator.getTypeName().equals(internalAuthenticatorName)) {
          internalAuthenticator = authenticator;
          break;
        }
      }
      if (internalAuthenticator == null) {
        throw new ISE("Could not locate internal authenticator with type name: %s", internalAuthenticatorName);
      }
    }
  }

  public HttpClient getEscalatedClient(HttpClient baseClient)
  {
    if (authConfig.isEnabled()) {
      return internalAuthenticator.createEscalatedClient(baseClient);
    } else {
      return baseClient;
    }
  }
}
