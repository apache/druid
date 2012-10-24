package com.metamx.druid.index.v1;

import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;

/**
 */
public interface Searchable
{
  public Iterable<SearchHit> searchDimensions(SearchQuery query, Filter filter);
}
