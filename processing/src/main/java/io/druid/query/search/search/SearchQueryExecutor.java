package io.druid.query.search.search;

import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Segment;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;

import java.util.List;

public abstract class SearchQueryExecutor
{
  protected final SearchQuery query;
  protected final SearchQuerySpec searchQuerySpec;
  protected final Segment segment;
  protected final List<DimensionSpec> dimsToSearch;

  public SearchQueryExecutor(SearchQuery query, Segment segment, List<DimensionSpec> dimensionSpecs) {
    this.query = query;
    this.segment = segment;
    this.searchQuerySpec = query.getQuery();
    this.dimsToSearch = dimensionSpecs;
  }

  public abstract Object2IntRBTreeMap<SearchHit> execute(int limit);
}
