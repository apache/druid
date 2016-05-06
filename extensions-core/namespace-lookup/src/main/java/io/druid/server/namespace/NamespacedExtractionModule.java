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
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.query.extraction.NamespaceLookupExtractorFactory;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractionModule implements DruidModule
{
  private static final String TYPE_PREFIX = "druid.query.extraction.namespace.cache.type";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("DruidNamespacedExtractionModule")
            .registerSubtypes(
                ExtractionNamespace.class,
                NamespaceLookupExtractorFactory.class
            )
    );
  }

  public static MapBinder<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> getNamespaceFactoryMapBinder(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends ExtractionNamespace>>()
        {
        },
        new TypeLiteral<ExtractionNamespaceCacheFactory<?>>()
        {
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
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
        .to(JDBCExtractionNamespaceCacheFactory.class)
        .in(LazySingleton.class);
    getNamespaceFactoryMapBinder(binder)
        .addBinding(URIExtractionNamespace.class)
        .to(URIExtractionNamespaceCacheFactory.class)
        .in(LazySingleton.class);
  }
}
