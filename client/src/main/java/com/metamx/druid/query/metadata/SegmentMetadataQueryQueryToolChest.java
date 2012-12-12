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

package com.metamx.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.metamx.common.guava.ConcatSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.ConcatQueryRunner;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SegmentMetadataResultValue;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.Interval;
import org.joda.time.Minutes;


public class SegmentMetadataQueryQueryToolChest implements QueryToolChest<Result<SegmentMetadataResultValue>, SegmentMetadataQuery>
{

  private static final TypeReference<Result<SegmentMetadataResultValue>> TYPE_REFERENCE = new TypeReference<Result<SegmentMetadataResultValue>>(){};

  @Override
  public QueryRunner<Result<SegmentMetadataResultValue>> mergeResults(final QueryRunner<Result<SegmentMetadataResultValue>> runner)
  {
    return new ConcatQueryRunner<Result<SegmentMetadataResultValue>>(Sequences.simple(ImmutableList.of(runner)));
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SegmentMetadataQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource())
        .setUser4(query.getType())
        .setUser5(Joiner.on(",").join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Sequence<Result<SegmentMetadataResultValue>> mergeSequences(Sequence<Sequence<Result<SegmentMetadataResultValue>>> seqOfSequences)
  {
    return new ConcatSequence<Result<SegmentMetadataResultValue>>(seqOfSequences);
  }

  @Override
  public Function<Result<SegmentMetadataResultValue>, Result<SegmentMetadataResultValue>> makeMetricManipulatorFn(
      SegmentMetadataQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SegmentMetadataResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SegmentMetadataResultValue>, SegmentMetadataQuery> getCacheStrategy(SegmentMetadataQuery query)
  {
    return null;
  }

  @Override
  public QueryRunner<Result<SegmentMetadataResultValue>> preMergeQueryDecoration(QueryRunner<Result<SegmentMetadataResultValue>> runner)
  {
    return runner;
  }

  @Override
  public QueryRunner<Result<SegmentMetadataResultValue>> postMergeQueryDecoration(QueryRunner<Result<SegmentMetadataResultValue>> runner)
  {
    return runner;
  }
}
