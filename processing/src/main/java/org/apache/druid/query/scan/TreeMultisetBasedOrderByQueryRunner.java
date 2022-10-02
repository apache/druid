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

package org.apache.druid.query.scan;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class TreeMultisetBasedOrderByQueryRunner extends OrderByQueryRunner
{
  public TreeMultisetBasedOrderByQueryRunner(ScanQueryEngine engine, Segment segment)
  {
    super(engine, segment);
  }

  @Override
  protected Sequence<ScanResultValue> getScanOrderByResultValueSequence(
      final ScanQuery query,
      final ResponseContext responseContext,
      final boolean legacy,
      final boolean hasTimeout,
      final long timeoutAt,
      final StorageAdapter adapter,
      final List<String> allColumns,
      final List<Interval> intervals,
      final SegmentId segmentId,
      final Filter filter,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    List<String> sortColumns = query.getOrderBys()
                                    .stream()
                                    .map(orderBy -> orderBy.getColumnName())
                                    .collect(Collectors.toList());
    List<String> orderByDirection = query.getOrderBys()
                                         .stream()
                                         .map(orderBy -> orderBy.getOrder().toString())
                                         .collect(Collectors.toList());

    return Sequences.concat(adapter.makeCursors(
        filter,
        intervals.get(0),
        query.getVirtualColumns(),
        Granularities.ALL,
        query.getTimeOrder().equals(ScanQuery.Order.DESCENDING) ||
        (query.getTimeOrder().equals(ScanQuery.Order.NONE) && query.isDescending()),
        queryMetrics
    ).map(cursor -> new TreeMultisetBasedSorterSequence(
        new TreeMultisetBasedSorterSequence.TreeMultisetBasedSorterIteratorMaker(
            sortColumns,
            legacy,
            cursor,
            hasTimeout,
            timeoutAt,
            query,
            segmentId,
            allColumns,
            orderByDirection
        )
    )));
  }
}
