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

package io.druid.guice.security;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.Authenticator;

public class AuthenticatorModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    final MapBinder<String, Authenticator> authenticatorMapBinder = PolyBind.optionBinder(
        binder,
        Key.get(Authenticator.class)
    );
    authenticatorMapBinder.addBinding(AuthConfig.ALLOW_ALL_NAME)
                          .to(AllowAllAuthenticator.class)
                          .in(LazySingleton.class);
  }

  @Provides
  @Named(AuthConfig.ALLOW_ALL_NAME)
  public Authenticator getAuthenticator()
  {
    return new AllowAllAuthenticator();
  }
}
