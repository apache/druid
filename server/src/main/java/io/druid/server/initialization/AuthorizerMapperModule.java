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
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.security.AllowAllAuthorizer;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AuthorizerMapperModule implements DruidModule
{
  private static final String AUTHORIZER_PROPERTIES_FORMAT_STRING = "druid.auth.authorizer.%s";
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
    private Properties props;
    private JsonConfigurator configurator;

    @Inject
    public void inject(Injector injector, Properties props, JsonConfigurator configurator)
    {
      this.authConfig = injector.getInstance(AuthConfig.class);
      this.injector = injector;
      this.props = props;
      this.configurator = configurator;
    }

    @Override
    public AuthorizerMapper get()
    {
      Map<String, Authorizer> authorizerMap = Maps.newHashMap();
      List<String> authorizers = authConfig.getAuthorizers();

      // Default is allow all
      if (authorizers == null) {
        return new AuthorizerMapper(null) {
          @Override
          public Authorizer getAuthorizer(String name)
          {
            return new AllowAllAuthorizer();
          }
        };
      }

      if (authorizers.isEmpty()) {
        throw new IAE("Must have at least one Authorizer configured.");
      }

      for (String authorizerName : authorizers) {
        final String authorizerPropertyBase = StringUtils.format(AUTHORIZER_PROPERTIES_FORMAT_STRING, authorizerName);
        final JsonConfigProvider<Authorizer> authorizerProvider = new JsonConfigProvider<>(
            authorizerPropertyBase,
            Authorizer.class
        );

        authorizerProvider.inject(props, configurator);

        Supplier<Authorizer> authorizerSupplier = authorizerProvider.get();
        if (authorizerSupplier == null) {
          throw new ISE("Could not create authorizer with name: %s", authorizerName);
        }
        Authorizer authorizer = authorizerSupplier.get();
        authorizerMap.put(authorizerName, authorizer);
      }

      return new AuthorizerMapper(authorizerMap);
    }
  }
}
