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

package io.druid.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.datasourcemetadata.DataSourceQueryQueryToolChest;
import io.druid.query.groupby.DefaultGroupByQueryMetricsFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryMetricsFactory;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import io.druid.query.timeseries.DefaultTimeseriesQueryMetricsFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryMetricsFactory;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.topn.DefaultTopNQueryMetricsFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryMetricsFactory;
import io.druid.query.topn.TopNQueryQueryToolChest;

import java.util.Map;

/**
 */
public class QueryToolChestModule implements Module
{
  public static final String GENERIC_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.generic.queryMetricsFactory";
  public static final String GROUPBY_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.groupBy.queryMetricsFactory";
  public static final String TIMESERIES_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.timeseries.queryMetricsFactory";
  public static final String TOPN_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.topN.queryMetricsFactory";

  public final Map<Class<? extends Query>, Class<? extends QueryToolChest>> mappings =
      ImmutableMap.<Class<? extends Query>, Class<? extends QueryToolChest>>builder()
                  .put(TimeseriesQuery.class, TimeseriesQueryQueryToolChest.class)
                  .put(SearchQuery.class, SearchQueryQueryToolChest.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryQueryToolChest.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryQueryToolChest.class)
                  .put(GroupByQuery.class, GroupByQueryQueryToolChest.class)
                  .put(SelectQuery.class, SelectQueryQueryToolChest.class)
                  .put(TopNQuery.class, TopNQueryQueryToolChest.class)
                  .put(DataSourceMetadataQuery.class, DataSourceQueryQueryToolChest.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryToolChest>> entry : mappings.entrySet()) {
      toolChests.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

    JsonConfigProvider.bind(binder, "druid.query.groupBy", GroupByQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.search", SearchQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.topN", TopNQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.segmentMetadata", SegmentMetadataQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.select", SelectQueryConfig.class);

    PolyBind.createChoice(
        binder,
        GENERIC_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(GenericQueryMetricsFactory.class),
        Key.get(DefaultGenericQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(GenericQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultGenericQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        GROUPBY_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(GroupByQueryMetricsFactory.class),
        Key.get(DefaultGroupByQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(GroupByQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultGroupByQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        TIMESERIES_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(TimeseriesQueryMetricsFactory.class),
        Key.get(DefaultTimeseriesQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(TimeseriesQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultTimeseriesQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        TOPN_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(TopNQueryMetricsFactory.class),
        Key.get(DefaultTopNQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(TopNQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultTopNQueryMetricsFactory.class);
  }
}
