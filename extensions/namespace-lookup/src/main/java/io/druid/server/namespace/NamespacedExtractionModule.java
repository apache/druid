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

package io.druid.server.namespace;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.metamx.common.IAE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.query.extraction.NamespacedExtractor;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.server.initialization.NamespaceLookupStaticConfig;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import io.druid.server.namespace.http.NamespacesCacheResource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractionModule implements DruidModule
{
  private static final Logger log = new Logger(NamespacedExtractionModule.class);
  private static final String TYPE_PREFIX = "druid.query.extraction.namespace.cache.type";
  private static final String STATIC_CONFIG_PREFIX = "druid.query.extraction.namespace";
  private final ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Function<String, List<String>>> reverseFnCache= new ConcurrentHashMap<>();

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("DruidNamespacedExtractionModule")
        {
          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(NamespacedExtractor.class);
            context.registerSubtypes(ExtractionNamespace.class);
          }
        }
    );
  }

  public static MapBinder<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> getNamespaceFactoryMapBinder(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends ExtractionNamespace>>()
        {
        },
        new TypeLiteral<ExtractionNamespaceFunctionFactory<?>>()
        {
        }
    );
  }

  @ManageLifecycle
  public static class NamespaceStaticConfiguration
  {
    private NamespaceLookupStaticConfig configuration;
    private NamespaceExtractionCacheManager manager;

    @Inject
    NamespaceStaticConfiguration(
        final NamespaceLookupStaticConfig configuration,
        final NamespaceExtractionCacheManager manager
    )
    {
      this.configuration = configuration;
      this.manager = manager;
    }

    @LifecycleStart
    public void start()
    {
      log.info("Loading configuration as static configuration");
      manager.scheduleOrUpdate(configuration.getNamespaces());
      log.info("Loaded %s namespace-lookup configuration", configuration.getNamespaces().size());
    }

    @LifecycleStop
    public void stop()
    {
      //NOOP
    }
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, STATIC_CONFIG_PREFIX, NamespaceLookupStaticConfig.class);
    PolyBind.createChoiceWithDefault(
        binder,
        TYPE_PREFIX,
        Key.get(NamespaceExtractionCacheManager.class),
        Key.get(OnHeapNamespaceExtractionCacheManager.class),
        "onheap"
    ).in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(NamespaceExtractionCacheManager.class))
        .addBinding("offheap")
        .to(OffHeapNamespaceExtractionCacheManager.class)
        .in(LazySingleton.class);

    getNamespaceFactoryMapBinder(binder)
        .addBinding(JDBCExtractionNamespace.class)
        .to(JDBCExtractionNamespaceFunctionFactory.class)
        .in(LazySingleton.class);
    getNamespaceFactoryMapBinder(binder)
        .addBinding(URIExtractionNamespace.class)
        .to(URIExtractionNamespaceFunctionFactory.class)
        .in(LazySingleton.class);

    LifecycleModule.register(binder, NamespaceStaticConfiguration.class);
    Jerseys.addResource(binder, NamespacesCacheResource.class);
  }


  @Provides
  @Named("namespaceVersionMap")
  @LazySingleton
  public ConcurrentMap<String, String> getVersionMap()
  {
    return new ConcurrentHashMap<>();
  }

  @Provides
  @Named("namespaceExtractionFunctionCache")
  public ConcurrentMap<String, Function<String, String>> getFnCache()
  {
    return fnCache;
  }

  @Provides
  @Named("namespaceReverseExtractionFunctionCache")
  public ConcurrentMap<String, Function<String, List<String>>> getReverseFnCache()
  {
    return reverseFnCache;
  }

  @Provides
  @Named("dimExtractionNamespace")
  @LazySingleton
  public Function<String, Function<String, String>> getFunctionMaker(
      @Named("namespaceExtractionFunctionCache")
      final ConcurrentMap<String, Function<String, String>> fnCache
  )
  {
    return new Function<String, Function<String, String>>()
    {
      @Nullable
      @Override
      public Function<String, String> apply(final String namespace)
      {
        Function<String, String> fn = fnCache.get(namespace);
        if (fn == null) {
          throw new IAE("Namespace [%s] not found", namespace);
        }
        return fn;
      }
    };
  }

  @Provides
  @Named("dimReverseExtractionNamespace")
  @LazySingleton
  public Function<String, Function<String, List<String>>> getReverseFunctionMaker(
      @Named("namespaceReverseExtractionFunctionCache")
      final ConcurrentMap<String, Function<String, List<String>>> reverseFn
  )
  {
    return new Function<String, Function<String, List<String>>>()
    {
      @Nullable
      @Override
      public Function<String, List<String>> apply(final String namespace)
      {
        Function<String, List<String>> fn = reverseFn.get(namespace);
        if (fn == null) {
          throw new IAE("Namespace reverse function [%s] not found", namespace);
        }
        return fn;
      }
    };
  }
}
