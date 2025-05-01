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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentMetadataQuerySegmentWalker;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;

/**
 * Module that binds dependencies required for segment schema management and
 * caching on the Coordinator.
 *
 * @see CentralizedDatasourceSchemaConfig
 * @see CoordinatorSegmentMetadataCache
 */
public class SegmentSchemaCacheModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.coordinator.segmentMetadata", SegmentMetadataQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.coordinator.query.scheduler", QuerySchedulerProvider.class, Global.class);
    JsonConfigProvider.bind(binder, "druid.coordinator.query.default", DefaultQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.coordinator.query.retryPolicy", RetryQueryRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.coordinator.internal.query.config", InternalQueryConfig.class);
    bindSchemaConfig(binder);

    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);
    toolChests.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryQueryToolChest.class);
    binder.bind(SegmentMetadataQueryQueryToolChest.class).in(LazySingleton.class);
    binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder =
        DruidBinders.queryRunnerFactoryBinder(binder);
    queryFactoryBinder.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryRunnerFactory.class);
    DruidBinders.queryBinder(binder);
    binder.bind(SegmentMetadataQueryRunnerFactory.class).in(LazySingleton.class);

    binder.bind(GenericQueryMetricsFactory.class).to(DefaultGenericQueryMetricsFactory.class);

    binder.bind(QueryScheduler.class)
          .toProvider(Key.get(QuerySchedulerProvider.class, Global.class))
          .in(LazySingleton.class);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    binder.bind(SegmentSchemaCache.class).in(LazySingleton.class);
    binder.bind(QuerySegmentWalker.class).to(SegmentMetadataQuerySegmentWalker.class).in(LazySingleton.class);

    LifecycleModule.register(binder, CoordinatorSegmentMetadataCache.class);
  }

  @LazySingleton
  @Provides
  public QueryWatcher getWatcher(QueryScheduler scheduler)
  {
    return scheduler;
  }

  public static void bindSchemaConfig(Binder binder)
  {
    JsonConfigProvider.bind(
        binder,
        CentralizedDatasourceSchemaConfig.PROPERTY_PREFIX,
        CentralizedDatasourceSchemaConfig.class
    );
  }
}
