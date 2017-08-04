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

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationManager;
import io.druid.server.security.AuthorizationManagerMapper;
import io.druid.server.security.DefaultAuthorizationManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AuthorizationManagerMapperModule implements DruidModule
{
  private static Logger log = new Logger(AuthorizationManagerMapperModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(AuthorizationManagerMapper.class)
          .toProvider(new AuthorizationManagerMapperProvider())
          .in(LazySingleton.class);

    LifecycleModule.register(binder, AuthorizationManagerMapper.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  private static class AuthorizationManagerMapperProvider implements Provider<AuthorizationManagerMapper>
  {
    private AuthConfig authConfig;
    private Injector injector;

    @Inject
    public void inject(Injector injector)
    {
      this.authConfig = injector.getInstance(AuthConfig.class);
      this.injector = injector;
    }

    @Override
    public AuthorizationManagerMapper get()
    {
      Map<String, AuthorizationManager> authorizationManagerMap = Maps.newHashMap();

      List<String> authorizationManagers = authConfig.getAuthorizationManagers();

      // If user didn't configure any AuthorizationManagers, use the default which rejects all requests.
      if (authorizationManagers == null || authorizationManagers.isEmpty()) {
        return new AuthorizationManagerMapper(null) {
          @Override
          public AuthorizationManager getAuthorizationManager(String namespace)
          {
            return new DefaultAuthorizationManager();
          }
        };
      }

      for (String authorizationManagerName : authorizationManagers) {
        AuthorizationManager authorizationManager = injector.getInstance(Key.get(
            AuthorizationManager.class,
            Names.named(authorizationManagerName)
        ));

        authorizationManagerMap.put(authorizationManager.getNamespace(), authorizationManager);
      }

      return new AuthorizationManagerMapper(authorizationManagerMap);
    }
  }
}
