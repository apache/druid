package com.metamx.druid;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchSortSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import org.joda.time.DateTime;

import java.util.TreeSet;

/**
 */
public class SearchBinaryFn implements BinaryFn<Result<SearchResultValue>, Result<SearchResultValue>, Result<SearchResultValue>>
{
  private final SearchSortSpec searchSortSpec;
  private final QueryGranularity gran;

  public SearchBinaryFn(
      SearchSortSpec searchSortSpec,
      QueryGranularity granularity
  )
  {
    this.searchSortSpec = searchSortSpec;
    this.gran = granularity;
  }

  @Override
  public Result<SearchResultValue> apply(Result<SearchResultValue> arg1, Result<SearchResultValue> arg2)
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    SearchResultValue arg1Vals = arg1.getValue();
    SearchResultValue arg2Vals = arg2.getValue();

    TreeSet<SearchHit> results = Sets.newTreeSet(searchSortSpec.getComparator());
    results.addAll(Lists.newArrayList(arg1Vals));
    results.addAll(Lists.newArrayList(arg2Vals));

    return (gran instanceof AllGranularity)
           ? new Result<SearchResultValue>(arg1.getTimestamp(), new SearchResultValue(Lists.newArrayList(results)))
           : new Result<SearchResultValue>(
               gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis())),
               new SearchResultValue(Lists.newArrayList(results))
           );
  }
}
