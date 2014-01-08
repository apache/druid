/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;

import java.util.Map;

/**
 */
public class QueryToolChestModule implements Module
{
  public final Map<Class<? extends Query>, Class<? extends QueryToolChest>> mappings =
      ImmutableMap.<Class<? extends Query>, Class<? extends QueryToolChest>>builder()
                  .put(TimeseriesQuery.class, TimeseriesQueryQueryToolChest.class)
                  .put(SearchQuery.class, SearchQueryQueryToolChest.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryQueryToolChest.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryQueryToolChest.class)
                  .put(GroupByQuery.class, GroupByQueryQueryToolChest.class)
                  .put(TopNQuery.class, TopNQueryQueryToolChest.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryToolChest>> entry : mappings.entrySet()) {
      toolChests.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    JsonConfigProvider.bind(binder, "druid.query", QueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.groupBy", GroupByQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.search", SearchQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.topN", TopNQueryConfig.class);
  }
}
