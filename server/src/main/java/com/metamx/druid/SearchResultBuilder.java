package com.metamx.druid;

import com.google.common.collect.Lists;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import org.joda.time.DateTime;


/**
 */
public class SearchResultBuilder
{
  private final DateTime timestamp;
  private final Iterable<SearchHit> hits;

  public SearchResultBuilder(
      DateTime timestamp,
      Iterable<SearchHit> hits
  )
  {
    this.timestamp = timestamp;
    this.hits = hits;
  }

  public Result<SearchResultValue> build()
  {
    return new Result<SearchResultValue>(
        timestamp,
        new SearchResultValue(Lists.newArrayList(hits))
    );
  }
}
