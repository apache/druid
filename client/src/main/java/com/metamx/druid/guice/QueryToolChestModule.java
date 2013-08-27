package com.metamx.druid.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.metamx.druid.Query;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.group.GroupByQueryConfig;
import com.metamx.druid.query.group.GroupByQueryQueryToolChest;
import com.metamx.druid.query.metadata.SegmentMetadataQuery;
import com.metamx.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.search.SearchQueryConfig;
import com.metamx.druid.query.search.SearchQueryQueryToolChest;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import com.metamx.druid.query.timeseries.TimeseriesQuery;
import com.metamx.druid.query.timeseries.TimeseriesQueryQueryToolChest;

import java.util.Map;

/**
 */
public class QueryToolChestModule implements Module
{
  final Map<Class<? extends Query>, Class<? extends QueryToolChest>> mappings =
      ImmutableMap.<Class<? extends Query>, Class<? extends QueryToolChest>>builder()
                  .put(TimeseriesQuery.class, TimeseriesQueryQueryToolChest.class)
                  .put(SearchQuery.class, SearchQueryQueryToolChest.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryQueryToolChest.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryQueryToolChest.class)
                  .put(GroupByQuery.class, GroupByQueryQueryToolChest.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = MapBinder.newMapBinder(
        binder, new TypeLiteral<Class<? extends Query>>(){}, new TypeLiteral<QueryToolChest>(){}
    );

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryToolChest>> entry : mappings.entrySet()) {
      toolChests.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    JsonConfigProvider.bind(binder, "druid.query.groupBy", GroupByQueryConfig.class);
    JsonConfigProvider.bind(binder, "druid.query.search", SearchQueryConfig.class);
  }
}
