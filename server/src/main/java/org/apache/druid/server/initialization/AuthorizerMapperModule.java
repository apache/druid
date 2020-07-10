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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class AuthorizerMapperModule implements DruidModule
{
  private static final String AUTHORIZER_PROPERTIES_FORMAT_STRING = "druid.auth.authorizer.%s";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(AuthorizerMapper.class)
          .toProvider(new AuthorizerMapperProvider())
          .in(LazySingleton.class);
    binder.bind(AuthValidator.class)
          .in(LazySingleton.class);
    LifecycleModule.register(binder, AuthorizerMapper.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
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
      Map<String, Authorizer> authorizerMap = new HashMap<>();
      List<String> authorizers = authConfig.getAuthorizers();

      validateAuthorizers(authorizers);

      // Default is allow all
      if (authorizers == null) {
        AllowAllAuthorizer allowAllAuthorizer = new AllowAllAuthorizer();
        authorizerMap.put(AuthConfig.ALLOW_ALL_NAME, allowAllAuthorizer);

        return new AuthorizerMapper(null)
        {
          @Override
          public Authorizer getAuthorizer(String name)
          {
            return allowAllAuthorizer;
          }

          @Override
          public Map<String, Authorizer> getAuthorizerMap()
          {
            return authorizerMap;
          }
        };
      }

      for (String authorizerName : authorizers) {
        final String authorizerPropertyBase = StringUtils.format(AUTHORIZER_PROPERTIES_FORMAT_STRING, authorizerName);
        final JsonConfigProvider<Authorizer> authorizerProvider = JsonConfigProvider.of(
            authorizerPropertyBase,
            Authorizer.class
        );

        String nameProperty = StringUtils.format("druid.auth.authorizer.%s.name", authorizerName);
        Properties adjustedProps = new Properties(props);
        if (adjustedProps.containsKey(nameProperty)) {
          throw new IAE("Name property [%s] is reserved.", nameProperty);
        } else {
          adjustedProps.put(nameProperty, authorizerName);
        }

        authorizerProvider.inject(adjustedProps, configurator);

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

  private static void validateAuthorizers(List<String> authorizers)
  {
    if (authorizers == null) {
      return;
    }

    if (authorizers.isEmpty()) {
      throw new IAE("Must have at least one Authorizer configured.");
    }

    Set<String> authorizerSet = new HashSet<>();
    for (String authorizer : authorizers) {
      if (authorizerSet.contains(authorizer)) {
        throw new ISE("Cannot have multiple authorizers with the same name: [%s]", authorizer);
      }
      authorizerSet.add(authorizer);
    }
  }
}
