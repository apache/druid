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

package org.apache.druid.guice.security;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.Authorizer;

public class AuthorizerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    final MapBinder<String, Authorizer> authorizerMapBinder = PolyBind.optionBinder(
        binder,
        Key.get(Authorizer.class)
    );
    authorizerMapBinder.addBinding(AuthConfig.ALLOW_ALL_NAME).to(AllowAllAuthorizer.class).in(LazySingleton.class);
  }

  @Provides
  @Named(AuthConfig.ALLOW_ALL_NAME)
  public Authorizer getAuthorizer()
  {
    return new AllowAllAuthorizer();
  }
}
