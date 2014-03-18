/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.incremental.IncrementalIndex;

import javax.annotation.Nullable;
import java.util.List;

public class GroupByQueryHelper
{
  public static Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> createIndexAccumulatorPair(
      final GroupByQuery query,
      final GroupByQueryConfig config
  )
  {
    final QueryGranularity gran = query.getGranularity();
    final long timeStart = query.getIntervals().get(0).getStartMillis();

    // use gran.iterable instead of gran.truncate so that
    // AllGranularity returns timeStart instead of Long.MIN_VALUE
    final long granTimeStart = gran.iterable(timeStart, timeStart + 1).iterator().next();

    final List<AggregatorFactory> aggs = Lists.transform(
        query.getAggregatorSpecs(),
        new Function<AggregatorFactory, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(@Nullable AggregatorFactory input)
          {
            return input.getCombiningFactory();
          }
        }
    );
    final List<String> dimensions = Lists.transform(
        query.getDimensions(),
        new Function<DimensionSpec, String>()
        {
          @Override
          public String apply(@Nullable DimensionSpec input)
          {
            return input.getOutputName();
          }
        }
    );
    IncrementalIndex index = new IncrementalIndex(
        // use granularity truncated min timestamp
        // since incoming truncated timestamps may precede timeStart
        granTimeStart,
        gran,
        aggs.toArray(new AggregatorFactory[aggs.size()])
    );

    Accumulator<IncrementalIndex, Row> accumulator = new Accumulator<IncrementalIndex, Row>()
    {
      @Override
      public IncrementalIndex accumulate(IncrementalIndex accumulated, Row in)
      {
        if (accumulated.add(Rows.toCaseInsensitiveInputRow(in, dimensions)) > config.getMaxResults()) {
          throw new ISE("Computation exceeds maxRows limit[%s]", config.getMaxResults());
        }

        return accumulated;
      }
    };
    return new Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>>(index, accumulator);
  }

}
