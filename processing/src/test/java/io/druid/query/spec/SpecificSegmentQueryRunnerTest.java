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

package io.druid.query.spec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
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
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
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
                                  .granularity(QueryGranularities.ALL)
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
                                  .granularity(QueryGranularities.ALL)
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
