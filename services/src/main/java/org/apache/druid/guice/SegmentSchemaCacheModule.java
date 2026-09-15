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
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.OptionalBinder;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentMetadataQuerySegmentWalker;

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
    JsonConfigProvider.bind(binder, "druid.coordinator.query.retryPolicy", RetryQueryRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.coordinator.internal.query.config", InternalQueryConfig.class);

    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);
    toolChests.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryQueryToolChest.class);
    binder.bind(SegmentMetadataQueryQueryToolChest.class).in(LazySingleton.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder =
        DruidBinders.queryRunnerFactoryBinder(binder);
    queryFactoryBinder.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryRunnerFactory.class);
    binder.bind(SegmentMetadataQueryRunnerFactory.class).in(LazySingleton.class);
    OptionalBinder.newOptionalBinder(binder, QuerySegmentWalker.class)
                  .setBinding()
                  .to(SegmentMetadataQuerySegmentWalker.class)
                  .in(LazySingleton.class);

    LifecycleModule.register(binder, CoordinatorSegmentMetadataCache.class);
  }
}
