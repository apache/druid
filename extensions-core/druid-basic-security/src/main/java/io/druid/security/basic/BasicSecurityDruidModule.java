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

package io.druid.security.basic;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.initialization.DruidModule;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.BasicHTTPEscalator;
import io.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheManager;
import io.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheNotifier;
import io.druid.security.basic.authentication.db.cache.CoordinatorBasicAuthenticatorCacheNotifier;
import io.druid.security.basic.authentication.db.cache.CoordinatorPollingBasicAuthenticatorCacheManager;
import io.druid.security.basic.authentication.db.cache.MetadataStoragePollingBasicAuthenticatorCacheManager;
import io.druid.security.basic.authentication.db.updater.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.authentication.endpoint.BasicAuthenticatorResource;
import io.druid.security.basic.authentication.endpoint.BasicAuthenticatorResourceHandler;
import io.druid.security.basic.authentication.endpoint.CoordinatorBasicAuthenticatorResourceHandler;
import io.druid.security.basic.authentication.endpoint.DefaultBasicAuthenticatorResourceHandler;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import io.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheNotifier;
import io.druid.security.basic.authorization.db.cache.CoordinatorBasicAuthorizerCacheNotifier;
import io.druid.security.basic.authorization.db.cache.CoordinatorPollingBasicAuthorizerCacheManager;
import io.druid.security.basic.authorization.db.cache.MetadataStoragePollingBasicAuthorizerCacheManager;
import io.druid.security.basic.authorization.db.updater.BasicAuthorizerMetadataStorageUpdater;
import io.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import io.druid.security.basic.authorization.endpoint.BasicAuthorizerResource;
import io.druid.security.basic.authorization.endpoint.BasicAuthorizerResourceHandler;
import io.druid.security.basic.authorization.endpoint.CoordinatorBasicAuthorizerResourceHandler;
import io.druid.security.basic.authorization.endpoint.DefaultBasicAuthorizerResourceHandler;

import java.util.List;

public class BasicSecurityDruidModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.auth.basic.common", BasicAuthCommonCacheConfig.class);

    LifecycleModule.register(binder, BasicAuthenticatorMetadataStorageUpdater.class);
    LifecycleModule.register(binder, BasicAuthorizerMetadataStorageUpdater.class);
    LifecycleModule.register(binder, BasicAuthenticatorCacheManager.class);
    LifecycleModule.register(binder, BasicAuthorizerCacheManager.class);

    Jerseys.addResource(binder, BasicAuthenticatorResource.class);
    Jerseys.addResource(binder, BasicAuthorizerResource.class);
  }

  @Provides @LazySingleton
  public static BasicAuthenticatorMetadataStorageUpdater createAuthenticatorStorageUpdater(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthenticatorMetadataStorageUpdater.class);
    } else {
      return null;
    }
  }

  @Provides @LazySingleton
  public static BasicAuthenticatorCacheManager createAuthenticatorCacheManager(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(MetadataStoragePollingBasicAuthenticatorCacheManager.class);
    } else {
      return injector.getInstance(CoordinatorPollingBasicAuthenticatorCacheManager.class);
    }
  }

  @Provides @LazySingleton
  public static BasicAuthenticatorResourceHandler createAuthenticatorResourceHandler(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthenticatorResourceHandler.class);
    } else {
      return injector.getInstance(DefaultBasicAuthenticatorResourceHandler.class);
    }
  }

  @Provides @LazySingleton
  public static BasicAuthenticatorCacheNotifier createAuthenticatorCacheNotifier(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthenticatorCacheNotifier.class);
    } else {
      return null;
    }
  }

  @Provides @LazySingleton
  public static BasicAuthorizerMetadataStorageUpdater createAuthorizerStorageUpdater(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthorizerMetadataStorageUpdater.class);
    } else {
      return null;
    }
  }

  @Provides @LazySingleton
  public static BasicAuthorizerCacheManager createAuthorizerCacheManager(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(MetadataStoragePollingBasicAuthorizerCacheManager.class);
    } else {
      return injector.getInstance(CoordinatorPollingBasicAuthorizerCacheManager.class);
    }
  }

  @Provides @LazySingleton
  public static BasicAuthorizerResourceHandler createAuthorizerResourceHandler(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthorizerResourceHandler.class);
    } else {
      return injector.getInstance(DefaultBasicAuthorizerResourceHandler.class);
    }
  }

  @Provides @LazySingleton
  public static BasicAuthorizerCacheNotifier createAuthorizerCacheNotifier(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthorizerCacheNotifier.class);
    } else {
      return null;
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("BasicDruidSecurity").registerSubtypes(
            BasicHTTPAuthenticator.class,
            BasicHTTPEscalator.class,
            BasicRoleBasedAuthorizer.class
        )
    );
  }

  private static boolean isCoordinator(Injector injector)
  {
    final String serviceName;
    try {
      serviceName = injector.getInstance(Key.get(String.class, Names.named("serviceName")));
    }
    catch (Exception e) {
      return false;
    }

    return "druid/coordinator".equals(serviceName);
  }
}
