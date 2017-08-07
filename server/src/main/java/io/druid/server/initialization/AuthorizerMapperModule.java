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
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.DefaultAuthorizer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AuthorizerMapperModule implements DruidModule
{
  private static Logger log = new Logger(AuthorizerMapperModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(AuthorizerMapper.class)
          .toProvider(new AuthorizerMapperProvider())
          .in(LazySingleton.class);

    LifecycleModule.register(binder, AuthorizerMapper.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  private static class AuthorizerMapperProvider implements Provider<AuthorizerMapper>
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
    public AuthorizerMapper get()
    {
      Map<String, Authorizer> authorizerMap = Maps.newHashMap();

      List<String> authorizers = authConfig.getAuthorizers();

      // If user didn't configure any Authorizers, use the default which rejects all requests.
      if (authorizers == null || authorizers.isEmpty()) {
        return new AuthorizerMapper(null) {
          @Override
          public Authorizer getAuthorizer(String namespace)
          {
            return new DefaultAuthorizer();
          }
        };
      }

      for (String authorizerName : authorizers) {
        Authorizer authorizer = injector.getInstance(Key.get(
            Authorizer.class,
            Names.named(authorizerName)
        ));

        authorizerMap.put(authorizer.getNamespace(), authorizer);
      }

      return new AuthorizerMapper(authorizerMap);
    }
  }
}
