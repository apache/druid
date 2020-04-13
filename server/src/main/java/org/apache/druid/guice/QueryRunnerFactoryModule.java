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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import org.apache.druid.query.datasourcemetadata.DataSourceMetadataQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryEngine;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQueryRunnerFactory;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;

import java.util.Map;

/**
 */
public class QueryRunnerFactoryModule extends QueryToolChestModule
{
  private static final Map<Class<? extends Query<?>>, Class<? extends QueryRunnerFactory<?, ?>>> MAPPINGS =
      ImmutableMap.<Class<? extends Query<?>>, Class<? extends QueryRunnerFactory<?, ?>>>builder()
                  .put(TimeseriesQuery.class, TimeseriesQueryRunnerFactory.class)
                  .put(SearchQuery.class, SearchQueryRunnerFactory.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryRunnerFactory.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryRunnerFactory.class)
                  .put(GroupByQuery.class, GroupByQueryRunnerFactory.class)
                  .put(ScanQuery.class, ScanQueryRunnerFactory.class)
                  .put(TopNQuery.class, TopNQueryRunnerFactory.class)
                  .put(DataSourceMetadataQuery.class, DataSourceMetadataQueryRunnerFactory.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    binder.bind(QueryScheduler.class)
          .toProvider(Key.get(QuerySchedulerProvider.class, Global.class))
          .in(LazySingleton.class);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.query.scheduler", QuerySchedulerProvider.class, Global.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder = DruidBinders.queryRunnerFactoryBinder(
        binder
    );

    for (Map.Entry<Class<? extends Query<?>>, Class<? extends QueryRunnerFactory<?, ?>>> entry : MAPPINGS.entrySet()) {
      queryFactoryBinder.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    binder.bind(GroupByQueryEngine.class).in(LazySingleton.class);
  }

  @LazySingleton
  @Provides
  public QueryWatcher getWatcher(QueryScheduler scheduler)
  {
    return scheduler;
  }
}
