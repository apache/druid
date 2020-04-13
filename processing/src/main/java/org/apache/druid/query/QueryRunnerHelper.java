/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;

/**
 */
public class QueryRunnerHelper
{
  public static <T> Sequence<Result<T>> makeCursorBasedQuery(
      final StorageAdapter adapter,
      final List<Interval> queryIntervals,
      final Filter filter,
      final VirtualColumns virtualColumns,
      final boolean descending,
      final Granularity granularity,
      final Function<Cursor, Result<T>> mapFn
  )
  {
    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    return Sequences.filter(
        Sequences.map(
            adapter.makeCursors(filter, queryIntervals.get(0), virtualColumns, granularity, descending, null),
            mapFn
        ),
        Objects::nonNull
    );
  }

  public static <T> QueryRunner<T> makeClosingQueryRunner(final QueryRunner<T> runner, final Closeable closeable)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
      {
        return Sequences.withBaggage(runner.run(queryPlus, responseContext), closeable);
      }
    };
  }
}
