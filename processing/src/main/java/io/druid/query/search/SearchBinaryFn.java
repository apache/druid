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
import com.google.common.collect.Sets;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchSortSpec;

import java.util.TreeSet;

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

    SearchResultValue arg1Vals = arg1.getValue();
    SearchResultValue arg2Vals = arg2.getValue();

    TreeSet<SearchHit> results = Sets.newTreeSet(searchSortSpec.getComparator());
    results.addAll(Lists.newArrayList(arg1Vals));
    results.addAll(Lists.newArrayList(arg2Vals));

    return (gran instanceof AllGranularity)
           ? new Result<SearchResultValue>(
        arg1.getTimestamp(), new SearchResultValue(
        Lists.newArrayList(
            Iterables.limit(results, limit)
        )
    )
    )
           : new Result<SearchResultValue>(
               gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis())),
               new SearchResultValue(Lists.newArrayList(results))
           );
  }
}
