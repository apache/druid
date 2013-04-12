/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchSortSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;

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
