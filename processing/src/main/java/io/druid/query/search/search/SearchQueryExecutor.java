package io.druid.query.search.search;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.search.SearchResultValue;
import io.druid.segment.Segment;
import io.druid.segment.data.Indexed;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public abstract class SearchQueryExecutor
{
  protected final SearchQuery query;
  protected final SearchQuerySpec searchQuerySpec;
  protected final Segment segment;
  protected final Iterable<DimensionSpec> dimsToSearch;
  protected final int limit;

  public SearchQueryExecutor(SearchQuery query, Segment segment) {
    this.query = query;
    this.segment = segment;
    this.searchQuerySpec = query.getQuery();
    this.dimsToSearch = getDimsToSearch(segment.asStorageAdapter().getAvailableDimensions(), query.getDimensions());
    this.limit = query.getLimit();
  }

  public abstract Sequence<Result<SearchResultValue>> execute();

  static Iterable<DimensionSpec> getDimsToSearch(Indexed<String> availableDimensions, List<DimensionSpec> dimensions)
  {
    if (dimensions == null || dimensions.isEmpty()) {
      return ImmutableList.copyOf(Iterables.transform(availableDimensions, Druids.DIMENSION_IDENTITY));
    } else {
      return dimensions;
    }
  }

  static Sequence<Result<SearchResultValue>> makeReturnResult(
      Segment segment, int limit, TreeMap<SearchHit, MutableInt> retVal)
  {
    Iterable<SearchHit> source = Iterables.transform(
        retVal.entrySet(), new Function<Entry<SearchHit, MutableInt>, SearchHit>()
        {
          @Override
          public SearchHit apply(Map.Entry<SearchHit, MutableInt> input)
          {
            SearchHit hit = input.getKey();
            return new SearchHit(hit.getDimension(), hit.getValue(), input.getValue().intValue());
          }
        }
    );
    return Sequences.simple(
        ImmutableList.of(
            new Result<>(
                segment.getDataInterval().getStart(),
                new SearchResultValue(
                    Lists.newArrayList(new FunctionalIterable<>(source).limit(limit))
                )
            )
        )
    );
  }
}
