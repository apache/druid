/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.common.utils.JodaUtils;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SegmentMetadataQueryQueryToolChest extends QueryToolChest<SegmentAnalysis, SegmentMetadataQuery>
{
  private static final TypeReference<SegmentAnalysis> TYPE_REFERENCE = new TypeReference<SegmentAnalysis>()
  {
  };
  private static final byte[] SEGMENT_METADATA_CACHE_PREFIX = new byte[]{0x4};

  private final SegmentMetadataQueryConfig config;

  @Inject
  public SegmentMetadataQueryQueryToolChest(
      SegmentMetadataQueryConfig config
  )
  {
    this.config = config;
  }

  @Override
  public QueryRunner<SegmentAnalysis> mergeResults(final QueryRunner<SegmentAnalysis> runner)
  {
    return new ResultMergeQueryRunner<SegmentAnalysis>(runner)
    {
      @Override
      protected Ordering<SegmentAnalysis> makeOrdering(Query<SegmentAnalysis> query)
      {
        if (((SegmentMetadataQuery) query).isMerge()) {
          // Merge everything always
          return new Ordering<SegmentAnalysis>()
          {
            @Override
            public int compare(
                @Nullable SegmentAnalysis left, @Nullable SegmentAnalysis right
            )
            {
              return 0;
            }
          };
        }

        return getOrdering(); // No two elements should be equal, so it should never merge
      }

      @Override
      protected BinaryFn<SegmentAnalysis, SegmentAnalysis, SegmentAnalysis> createMergeFn(final Query<SegmentAnalysis> inQ)
      {
        return new BinaryFn<SegmentAnalysis, SegmentAnalysis, SegmentAnalysis>()
        {
          private final SegmentMetadataQuery query = (SegmentMetadataQuery) inQ;

          @Override
          public SegmentAnalysis apply(SegmentAnalysis arg1, SegmentAnalysis arg2)
          {
            if (arg1 == null) {
              return arg2;
            }

            if (arg2 == null) {
              return arg1;
            }

            List<Interval> newIntervals = JodaUtils.condenseIntervals(
                Iterables.concat(arg1.getIntervals(), arg2.getIntervals())
            );

            final Map<String, ColumnAnalysis> leftColumns = arg1.getColumns();
            final Map<String, ColumnAnalysis> rightColumns = arg2.getColumns();
            Map<String, ColumnAnalysis> columns = Maps.newTreeMap();

            Set<String> rightColumnNames = Sets.newHashSet(rightColumns.keySet());
            for (Map.Entry<String, ColumnAnalysis> entry : leftColumns.entrySet()) {
              final String columnName = entry.getKey();
              columns.put(columnName, entry.getValue().fold(rightColumns.get(columnName)));
              rightColumnNames.remove(columnName);
            }

            for (String columnName : rightColumnNames) {
              columns.put(columnName, rightColumns.get(columnName));
            }

            return new SegmentAnalysis("merged", newIntervals, columns, arg1.getSize() + arg2.getSize());
          }
        };
      }
    };
  }

  @Override
  public Sequence<SegmentAnalysis> mergeSequences(Sequence<Sequence<SegmentAnalysis>> seqOfSequences)
  {
    return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public Sequence<SegmentAnalysis> mergeSequencesUnordered(Sequence<Sequence<SegmentAnalysis>> seqOfSequences)
  {
    return new MergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SegmentMetadataQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<SegmentAnalysis, SegmentAnalysis> makePreComputeManipulatorFn(
      SegmentMetadataQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<SegmentAnalysis> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<SegmentAnalysis, SegmentAnalysis, SegmentMetadataQuery> getCacheStrategy(SegmentMetadataQuery query)
  {
    return new CacheStrategy<SegmentAnalysis, SegmentAnalysis, SegmentMetadataQuery>()
    {
      @Override
      public byte[] computeCacheKey(SegmentMetadataQuery query)
      {
        byte[] includerBytes = query.getToInclude().getCacheKey();
        byte[] analysisTypesBytes = query.getAnalysisTypesCacheKey();
        return ByteBuffer.allocate(1 + includerBytes.length + analysisTypesBytes.length)
                         .put(SEGMENT_METADATA_CACHE_PREFIX)
                         .put(includerBytes)
                         .put(analysisTypesBytes)
                         .array();
      }

      @Override
      public TypeReference<SegmentAnalysis> getCacheObjectClazz()
      {
        return getResultTypeReference();
      }

      @Override
      public Function<SegmentAnalysis, SegmentAnalysis> prepareForCache()
      {
        return new Function<SegmentAnalysis, SegmentAnalysis>()
        {
          @Override
          public SegmentAnalysis apply(@Nullable SegmentAnalysis input)
          {
            return input;
          }
        };
      }

      @Override
      public Function<SegmentAnalysis, SegmentAnalysis> pullFromCache()
      {
        return new Function<SegmentAnalysis, SegmentAnalysis>()
        {
          @Override
          public SegmentAnalysis apply(@Nullable SegmentAnalysis input)
          {
            return input;
          }
        };
      }

      @Override
      public Sequence<SegmentAnalysis> mergeSequences(Sequence<Sequence<SegmentAnalysis>> seqOfSequences)
      {
        return new MergeSequence<SegmentAnalysis>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(SegmentMetadataQuery query, List<T> segments)
  {
    if (!query.isUsingDefaultInterval()) {
      return segments;
    }

    if (segments.size() <= 1) {
      return segments;
    }

    final T max = segments.get(segments.size() - 1);

    DateTime targetEnd = max.getInterval().getEnd();
    final Interval targetInterval = new Interval(config.getDefaultHistory(), targetEnd);

    return Lists.newArrayList(
        Iterables.filter(
            segments,
            new Predicate<T>()
            {
              @Override
              public boolean apply(T input)
              {
                return (input.getInterval().overlaps(targetInterval));
              }
            }
        )
    );
  }

  private Ordering<SegmentAnalysis> getOrdering()
  {
    return new Ordering<SegmentAnalysis>()
    {
      @Override
      public int compare(SegmentAnalysis left, SegmentAnalysis right)
      {
        return left.getId().compareTo(right.getId());
      }
    }.nullsFirst();
  }
}
