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
import com.google.common.collect.Maps;
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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class AuthenticatorMapperModule implements DruidModule
{
  private static final String AUTHENTICATOR_PROPERTIES_FORMAT_STRING = "druid.auth.authenticator.%s";
  private static Logger log = new Logger(AuthenticatorMapperModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(AuthenticatorMapper.class)
          .toProvider(new AuthenticatorMapperProvider())
          .in(LazySingleton.class);

    LifecycleModule.register(binder, AuthenticatorMapper.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  private static class AuthenticatorMapperProvider implements Provider<AuthenticatorMapper>
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
    public AuthenticatorMapper get()
    {
      // order of the authenticators matters
      Map<String, Authenticator> authenticatorMap = Maps.newLinkedHashMap();

      List<String> authenticators = authConfig.getAuthenticatorChain();

      validateAuthenticators(authenticators);

      // Default configuration is to allow all requests.
      if (authenticators == null) {
        authenticatorMap.put(AuthConfig.ALLOW_ALL_NAME, new AllowAllAuthenticator());
        return new AuthenticatorMapper(authenticatorMap);
      }

      for (String authenticatorName : authenticators) {
        final String authenticatorPropertyBase = StringUtils.format(AUTHENTICATOR_PROPERTIES_FORMAT_STRING, authenticatorName);
        final JsonConfigProvider<Authenticator> authenticatorProvider = JsonConfigProvider.of(
            authenticatorPropertyBase,
            Authenticator.class
        );

        String nameProperty = StringUtils.format("druid.auth.authenticator.%s.name", authenticatorName);
        Properties adjustedProps = new Properties(props);
        if (adjustedProps.containsKey(nameProperty)) {
          throw new IAE("Name property [%s] is reserved.", nameProperty);
        } else {
          adjustedProps.put(nameProperty, authenticatorName);
        }
        authenticatorProvider.inject(adjustedProps, configurator);

        Supplier<Authenticator> authenticatorSupplier = authenticatorProvider.get();
        if (authenticatorSupplier == null) {
          throw new ISE("Could not create authenticator with name: %s", authenticatorName);
        }
        Authenticator authenticator = authenticatorSupplier.get();
        authenticatorMap.put(authenticatorName, authenticator);
      }

      return new AuthenticatorMapper(authenticatorMap);
    }
  }

  private static void validateAuthenticators(List<String> authenticators)
  {
    if (authenticators == null) {
      return;
    }

    if (authenticators.isEmpty()) {
      throw new IAE("Must have at least one Authenticator configured.");
    }

    Set<String> authenticatorSet = new HashSet<>();
    for (String authenticator : authenticators) {
      if (authenticatorSet.contains(authenticator)) {
        throw new ISE("Cannot have multiple authenticators with the same name: [%s]", authenticator);
      }
      authenticatorSet.add(authenticator);
    }
  }
}
