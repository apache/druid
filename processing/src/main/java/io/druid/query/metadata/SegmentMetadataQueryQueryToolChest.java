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

package io.druid.query.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.common.utils.JodaUtils;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.joda.time.Interval;
import org.joda.time.Minutes;

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

            if (!query.isMerge()) {
              throw new ISE("Merging when a merge isn't supposed to happen[%s], [%s]", arg1, arg2);
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
    return new OrderedMergeSequence<SegmentAnalysis>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SegmentMetadataQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource().toString())
        .setUser4(query.getType())
        .setUser5(Joiner.on(",").join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser9(Minutes.minutes(numMinutes).toString());
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
        return ByteBuffer.allocate(1 + includerBytes.length)
            .put(SEGMENT_METADATA_CACHE_PREFIX)
            .put(includerBytes)
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
