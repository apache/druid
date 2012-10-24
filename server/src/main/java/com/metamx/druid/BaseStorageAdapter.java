package com.metamx.druid;

import com.google.common.collect.Sets;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.ConciseOffset;
import com.metamx.druid.index.v1.processing.IntersectingOffset;
import com.metamx.druid.index.v1.processing.Offset;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.search.SearchQuerySpec;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.List;
import java.util.TreeSet;

/**
 */
public abstract class BaseStorageAdapter implements StorageAdapter
{
  public abstract Indexed<String> getAvailableDimensions();

  public abstract Indexed<String> getDimValueLookup(String dimension);

  public abstract ImmutableConciseSet getInvertedIndex(String dimension, String dimVal);

  public abstract Offset getFilterOffset(Filter filter);

  @Override
  public Iterable<SearchHit> searchDimensions(final SearchQuery query, final Filter filter)
  {
    final List<String> dimensions = query.getDimensions();
    final SearchQuerySpec searchQuerySpec = query.getQuery();

    final TreeSet<SearchHit> retVal = Sets.newTreeSet(
        searchQuerySpec.getSearchSortSpec()
                       .getComparator()
    );

    Iterable<String> dimsToSearch;
    if (dimensions == null || dimensions.isEmpty()) {
      dimsToSearch = getAvailableDimensions();
    } else {
      dimsToSearch = dimensions;
    }

    Offset filterOffset = (filter == null) ? null : getFilterOffset(filter);

    for (String dimension : dimsToSearch) {
      Iterable<String> dims = getDimValueLookup(dimension);
      if (dims != null) {
        for (String dimVal : dims) {
          if (searchQuerySpec.accept(dimVal)) {
            if (filterOffset != null) {
              Offset lhs = new ConciseOffset(getInvertedIndex(dimension, dimVal));
              Offset rhs = filterOffset.clone();

              if (new IntersectingOffset(lhs, rhs).withinBounds()) {
                retVal.add(new SearchHit(dimension, dimVal));
              }
            } else {
              retVal.add(new SearchHit(dimension, dimVal));
            }
          }
        }
      }
    }

    return new FunctionalIterable<SearchHit>(retVal).limit(query.getLimit());
  }
}

