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
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import org.apache.druid.query.datasourcemetadata.DataSourceQueryQueryToolChest;
import org.apache.druid.query.groupby.DefaultGroupByQueryMetricsFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryMetricsFactory;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.search.DefaultSearchQueryMetricsFactory;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQueryConfig;
import org.apache.druid.query.search.SearchQueryMetricsFactory;
import org.apache.druid.query.search.SearchQueryQueryToolChest;
import org.apache.druid.query.select.DefaultSelectQueryMetricsFactory;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.query.select.SelectQueryMetricsFactory;
import org.apache.druid.query.select.SelectQueryQueryToolChest;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import org.apache.druid.query.timeseries.DefaultTimeseriesQueryMetricsFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryMetricsFactory;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.topn.DefaultTopNQueryMetricsFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryMetricsFactory;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;

import java.util.Map;

/**
 */
public class QueryToolChestModule implements Module
{
  public static final String GENERIC_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.generic.queryMetricsFactory";
  public static final String GROUPBY_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.groupBy.queryMetricsFactory";
  public static final String TIMESERIES_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.timeseries.queryMetricsFactory";
  public static final String TOPN_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.topN.queryMetricsFactory";
  public static final String SELECT_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.select.queryMetricsFactory";
  public static final String SEARCH_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.search.queryMetricsFactory";

  public final Map<Class<? extends Query>, Class<? extends QueryToolChest>> mappings =
      ImmutableMap.<Class<? extends Query>, Class<? extends QueryToolChest>>builder()
                  .put(TimeseriesQuery.class, TimeseriesQueryQueryToolChest.class)
                  .put(SearchQuery.class, SearchQueryQueryToolChest.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryQueryToolChest.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryQueryToolChest.class)
                  .put(GroupByQuery.class, GroupByQueryQueryToolChest.class)
                  .put(ScanQuery.class, ScanQueryQueryToolChest.class)
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

    JsonConfigProvider.bind(binder, "druid.query", QueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.groupBy", GroupByQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.search", SearchQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.topN", TopNQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.segmentMetadata", SegmentMetadataQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.select", SelectQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.scan", ScanQueryConfig.class);

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

    PolyBind.createChoice(
        binder,
        SELECT_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(SelectQueryMetricsFactory.class),
        Key.get(DefaultSelectQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(SelectQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultSelectQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        SEARCH_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(SearchQueryMetricsFactory.class),
        Key.get(DefaultSearchQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(SearchQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultSearchQueryMetricsFactory.class);
  }
}
