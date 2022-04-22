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

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.server.security.AuthConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class DruidAuthModuleTest
{
  private Injector injector;
  private DruidAuthModule authModule;

  @Before
  public void setup()
  {
    authModule = new DruidAuthModule();
    injector = Guice.createInjector(
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
        },
        authModule
    );
  }

  @Test
  public void testAuthConfigSingleton()
  {
    AuthConfig config1 = injector.getInstance(AuthConfig.class);
    AuthConfig config2 = injector.getInstance(AuthConfig.class);
    Assert.assertNotNull(config1);
    Assert.assertSame(config1, config2);
  }

  @Test
  public void testAuthConfigDefault()
  {
    Properties properties = new Properties();
    final AuthConfig authConfig = injectProperties(properties);
    Assert.assertNotNull(authConfig);
    Assert.assertNull(authConfig.getAuthenticatorChain());
    Assert.assertNull(authConfig.getAuthorizers());
    Assert.assertTrue(authConfig.getUnsecuredPaths().isEmpty());
    Assert.assertFalse(authConfig.isAllowUnauthenticatedHttpOptions());
    Assert.assertFalse(authConfig.authorizeQueryContextParams());
  }

  @Test
  public void testAuthConfigSet()
  {
    Properties properties = new Properties();
    properties.setProperty("druid.auth.authenticatorChain", "[\"chain\", \"of\", \"authenticators\"]");
    properties.setProperty("druid.auth.authorizers", "[\"authorizers\", \"list\"]");
    properties.setProperty("druid.auth.unsecuredPaths", "[\"path1\", \"path2\"]");
    properties.setProperty("druid.auth.allowUnauthenticatedHttpOptions", "true");
    properties.setProperty("druid.auth.authorizeQueryContextParams", "true");

    final AuthConfig authConfig = injectProperties(properties);
    Assert.assertNotNull(authConfig);
    Assert.assertEquals(ImmutableList.of("chain", "of", "authenticators"), authConfig.getAuthenticatorChain());
    Assert.assertEquals(ImmutableList.of("authorizers", "list"), authConfig.getAuthorizers());
    Assert.assertEquals(ImmutableList.of("path1", "path2"), authConfig.getUnsecuredPaths());
    Assert.assertTrue(authConfig.authorizeQueryContextParams());
  }

  private AuthConfig injectProperties(Properties properties)
  {
    final JsonConfigProvider<AuthConfig> provider = JsonConfigProvider.of(
        "druid.auth",
        AuthConfig.class
    );
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    return provider.get().get();
  }
}
