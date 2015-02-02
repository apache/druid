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

package io.druid.query.spec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultBuilder;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.SegmentMissingException;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SpecificSegmentQueryRunnerTest
{
  @Test
  public void testRetry() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    SegmentDescriptor descriptor = new SegmentDescriptor(
        new Interval("2012-01-01T00:00:00Z/P1D"),
        "version",
        0
    );

    final SpecificSegmentQueryRunner queryRunner = new SpecificSegmentQueryRunner(
        new QueryRunner()
        {
          @Override
          public Sequence run(Query query, Map responseContext)
          {
            return new Sequence()
            {
              @Override
              public Object accumulate(Object initValue, Accumulator accumulator)
              {
                throw new SegmentMissingException("FAILSAUCE");
              }

              @Override
              public Yielder<Object> toYielder(
                  Object initValue, YieldingAccumulator accumulator
              )
              {
                return null;
              }
            };

          }
        },
        new SpecificSegmentSpec(
            descriptor
        )
    );

    final Map<String, Object> responseContext = Maps.newHashMap();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("foo")
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(ImmutableList.of(new Interval("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
                                  .build();
    Sequence results = queryRunner.run(
        query,
        responseContext
    );
    Sequences.toList(results, Lists.newArrayList());

    Object missingSegments = responseContext.get(Result.MISSING_SEGMENTS_KEY);

    Assert.assertTrue(missingSegments != null);
    Assert.assertTrue(missingSegments instanceof List);

    Object segmentDesc = ((List) missingSegments).get(0);

    Assert.assertTrue(segmentDesc instanceof SegmentDescriptor);

    SegmentDescriptor newDesc = mapper.readValue(mapper.writeValueAsString(segmentDesc), SegmentDescriptor.class);

    Assert.assertEquals(descriptor, newDesc);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRetry2() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    SegmentDescriptor descriptor = new SegmentDescriptor(
        new Interval("2012-01-01T00:00:00Z/P1D"),
        "version",
        0
    );

    TimeseriesResultBuilder builder = new TimeseriesResultBuilder(
        new DateTime("2012-01-01T00:00:00Z")
    );
    CountAggregator rows = new CountAggregator("rows");
    rows.aggregate();
    builder.addMetric(rows);
    final Result<TimeseriesResultValue> value = builder.build();

    final SpecificSegmentQueryRunner queryRunner = new SpecificSegmentQueryRunner(
        new QueryRunner()
        {
          @Override
          public Sequence run(Query query, Map responseContext)
          {
            return Sequences.withEffect(
                Sequences.simple(Arrays.asList(value)),
                new Runnable()
                {
                  @Override
                  public void run()
                  {
                    throw new SegmentMissingException("FAILSAUCE");
                  }
                },
                MoreExecutors.sameThreadExecutor()
            );
          }
        },
        new SpecificSegmentSpec(
            descriptor
        )
    );

    final Map<String, Object> responseContext = Maps.newHashMap();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("foo")
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(ImmutableList.of(new Interval("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
                                  .build();
    Sequence results = queryRunner.run(
        query,
        responseContext
    );
    List<Result<TimeseriesResultValue>> res = Sequences.toList(
        results,
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertEquals(1, res.size());

    Result<TimeseriesResultValue> theVal = res.get(0);

    Assert.assertTrue(1L == theVal.getValue().getLongMetric("rows"));

    Object missingSegments = responseContext.get(Result.MISSING_SEGMENTS_KEY);

    Assert.assertTrue(missingSegments != null);
    Assert.assertTrue(missingSegments instanceof List);

    Object segmentDesc = ((List) missingSegments).get(0);

    Assert.assertTrue(segmentDesc instanceof SegmentDescriptor);

    SegmentDescriptor newDesc = mapper.readValue(mapper.writeValueAsString(segmentDesc), SegmentDescriptor.class);

    Assert.assertEquals(descriptor, newDesc);
  }
}