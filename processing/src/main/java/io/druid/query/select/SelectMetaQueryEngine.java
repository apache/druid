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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.filter.Filter;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class SelectMetaQueryEngine
{
  public Sequence<Result<SelectMetaResultValue>> process(final SelectMetaQuery query, final Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final String identifier = segment.getIdentifier();
    final Filter filter = Filters.toFilter(query.getDimensionsFilter());

    StorageAdapter storageAdapter = segment.asStorageAdapter();
    final List<String> dimensions = Lists.newArrayList(storageAdapter.getAvailableDimensions());
    final List<String> metrics = Lists.newArrayList(storageAdapter.getAvailableMetrics());

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        intervals,
        VirtualColumns.EMPTY,
        filter,
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<SelectMetaResultValue>>()
        {
          @Override
          public Result<SelectMetaResultValue> apply(Cursor cursor)
          {
            int i = 0;
            for (; !cursor.isDone(); cursor.advance()) {
              i++;
            }
            return new Result(
                cursor.getTime(),
                new SelectMetaResultValue(dimensions, metrics, ImmutableMap.of(identifier, i))
            );
          }
        }
    );
  }
}
