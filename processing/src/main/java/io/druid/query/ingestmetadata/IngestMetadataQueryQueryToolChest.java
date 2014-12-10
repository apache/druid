/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.ingestmetadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.query.BySegmentSkippingQueryRunner;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSourceUtil;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.timeline.LogicalSegment;

import java.util.List;
import java.util.Map;

/**
 */
public class IngestMetadataQueryQueryToolChest
    extends QueryToolChest<Result<IngestMetadataResultValue>, IngestMetadataQuery>
{
  private static final TypeReference<Result<IngestMetadataResultValue>> TYPE_REFERENCE = new TypeReference<Result<IngestMetadataResultValue>>()
  {
  };

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(IngestMetadataQuery query, List<T> segments)
  {
    if (segments.size() <= 1) {
      return segments;
    }

    final T min = segments.get(0);
    final T max = segments.get(segments.size() - 1);

    return Lists.newArrayList(
        Iterables.filter(
            segments,
            new Predicate<T>()
            {
              @Override
              public boolean apply(T input)
              {
                return (min != null && input.getInterval().overlaps(min.getInterval())) ||
                       (max != null && input.getInterval().overlaps(max.getInterval()));
              }
            }
        )
    );
  }

  @Override
  public QueryRunner<Result<IngestMetadataResultValue>> mergeResults(
      final QueryRunner<Result<IngestMetadataResultValue>> runner
  )
  {
    return new BySegmentSkippingQueryRunner<Result<IngestMetadataResultValue>>(runner)
    {
      @Override
      protected Sequence<Result<IngestMetadataResultValue>> doRun(
          QueryRunner<Result<IngestMetadataResultValue>> baseRunner,
          Query<Result<IngestMetadataResultValue>> input,
          Map<String, Object> context
      )
      {
        IngestMetadataQuery query = (IngestMetadataQuery) input;
        return Sequences.simple(
            query.mergeResults(
                Sequences.toList(
                    baseRunner.run(query, context),
                    Lists.<Result<IngestMetadataResultValue>>newArrayList()
                )
            )
        );
      }
    };
  }

  @Override
  public Sequence<Result<IngestMetadataResultValue>> mergeSequences(Sequence<Sequence<Result<IngestMetadataResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public Sequence<Result<IngestMetadataResultValue>> mergeSequencesUnordered(Sequence<Sequence<Result<IngestMetadataResultValue>>> seqOfSequences)
  {
    return new MergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(IngestMetadataQuery query)
  {
    return new ServiceMetricEvent.Builder()
        .setUser2(DataSourceUtil.getMetricName(query.getDataSource()))
        .setUser4(query.getType())
        .setUser6("false");
  }

  @Override
  public Function<Result<IngestMetadataResultValue>, Result<IngestMetadataResultValue>> makePreComputeManipulatorFn(
      IngestMetadataQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<IngestMetadataResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy getCacheStrategy(IngestMetadataQuery query)
  {
    return null;
  }

  public Ordering<Result<IngestMetadataResultValue>> getOrdering()
  {
    return Ordering.natural();
  }
}
