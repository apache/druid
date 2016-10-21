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

package io.druid.query.search;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.Result;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchSortSpec;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

/**
 */
public class SearchBinaryFn
    implements BinaryFn<Result<SearchResultValue>, Result<SearchResultValue>, Result<SearchResultValue>>
{
  private final SearchSortSpec searchSortSpec;
  private final QueryGranularity gran;
  private final int limit;

  public SearchBinaryFn(
      SearchSortSpec searchSortSpec,
      QueryGranularity granularity,
      int limit
  )
  {
    this.searchSortSpec = searchSortSpec;
    this.gran = granularity;
    this.limit = limit;
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

    final int limit = gran instanceof AllGranularity ? this.limit : -1;

    SearchResultValue arg1Vals = arg1.getValue();
    SearchResultValue arg2Vals = arg2.getValue();

    Iterable<SearchHit> merged = Iterables.mergeSorted(
        Arrays.asList(arg1Vals, arg2Vals),
        searchSortSpec.getComparator()
    );

    int maxSize = arg1Vals.getValue().size() + arg2Vals.getValue().size();
    if (limit > 0) {
      maxSize = Math.min(limit, maxSize);
    }
    List<SearchHit> results = Lists.newArrayListWithExpectedSize(maxSize);

    SearchHit prev = null;
    for (SearchHit searchHit : merged) {
      if (prev == null) {
        prev = searchHit;
        continue;
      }
      if (prev.equals(searchHit)) {
        if (prev.getCount() != null && searchHit.getCount() != null) {
          prev = new SearchHit(
              prev.getDimension(),
              prev.getValue(),
              prev.getCount() + searchHit.getCount()
          );
        } else {
          prev = new SearchHit(
                  prev.getDimension(),
                  prev.getValue()
          );
        }
      } else {
        results.add(prev);
        prev = searchHit;
        if (limit > 0 && results.size() >= limit) {
          break;
        }
      }
    }

    if (prev != null && (limit < 0 || results.size() < limit)) {
      results.add(prev);
    }

    final DateTime timestamp = gran instanceof AllGranularity
                               ? arg1.getTimestamp()
                               : gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis()));

    return new Result<SearchResultValue>(timestamp, new SearchResultValue(results));
  }
}
