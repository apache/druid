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

package org.apache.druid.query.spec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.CountAggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultBuilder;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.SegmentMissingException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SpecificSegmentQueryRunnerTest
{
  @Test
  public void testRetry() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    SegmentDescriptor descriptor = new SegmentDescriptor(
        Intervals.of("2012-01-01T00:00:00Z/P1D"),
        "version",
        0
    );

    final SpecificSegmentQueryRunner queryRunner = new SpecificSegmentQueryRunner(
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
          {
            return new Sequence()
            {
              @Override
              public Object accumulate(Object initValue, Accumulator accumulator)
              {
                throw new SegmentMissingException("FAILSAUCE");
              }

              @Override
              public Yielder<Object> toYielder(Object initValue, YieldingAccumulator accumulator)
              {
                throw new SegmentMissingException("FAILSAUCE");
              }
            };

          }
        },
        new SpecificSegmentSpec(
            descriptor
        )
    );

    // from accumulate
    ResponseContext responseContext = ResponseContext.createEmpty();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("foo")
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(Intervals.of("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
                                  .build();
    Sequence results = queryRunner.run(QueryPlus.wrap(query), responseContext);
    results.toList();
    validate(mapper, descriptor, responseContext);

    // from toYielder
    responseContext = ResponseContext.createEmpty();
    results = queryRunner.run(QueryPlus.wrap(query), responseContext);
    results.toYielder(
        null,
        new YieldingAccumulator()
        {
          final List lists = new ArrayList<>();

          @Override
          public Object accumulate(Object accumulated, Object in)
          {
            lists.add(in);
            return in;
          }
        }
    );
    validate(mapper, descriptor, responseContext);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRetry2() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    SegmentDescriptor descriptor = new SegmentDescriptor(
        Intervals.of("2012-01-01T00:00:00Z/P1D"),
        "version",
        0
    );

    TimeseriesResultBuilder builder = new TimeseriesResultBuilder(
        DateTimes.of("2012-01-01T00:00:00Z")
    );
    CountAggregator rows = new CountAggregator();
    rows.aggregate();
    builder.addMetric("rows", rows.get());
    final Result<TimeseriesResultValue> value = builder.build();

    final SpecificSegmentQueryRunner queryRunner = new SpecificSegmentQueryRunner(
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
          {
            return Sequences.withEffect(
                Sequences.simple(Collections.singletonList(value)),
                new Runnable()
                {
                  @Override
                  public void run()
                  {
                    throw new SegmentMissingException("FAILSAUCE");
                  }
                },
                Execs.directExecutor()
            );
          }
        },
        new SpecificSegmentSpec(
            descriptor
        )
    );

    final ResponseContext responseContext = ResponseContext.createEmpty();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("foo")
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(Intervals.of("2012-01-01T00:00:00Z/P1D")))
                                  .aggregators(
                                      ImmutableList.of(
                                          new CountAggregatorFactory("rows")
                                      )
                                  )
                                  .build();
    Sequence results = queryRunner.run(QueryPlus.wrap(query), responseContext);
    List<Result<TimeseriesResultValue>> res = results.toList();

    Assert.assertEquals(1, res.size());

    Result<TimeseriesResultValue> theVal = res.get(0);

    Assert.assertTrue(1L == theVal.getValue().getLongMetric("rows"));

    validate(mapper, descriptor, responseContext);
  }

  private void validate(ObjectMapper mapper, SegmentDescriptor descriptor, ResponseContext responseContext)
      throws IOException
  {
    Object missingSegments = responseContext.get(ResponseContext.Key.MISSING_SEGMENTS);

    Assert.assertTrue(missingSegments != null);
    Assert.assertTrue(missingSegments instanceof List);

    Object segmentDesc = ((List) missingSegments).get(0);

    Assert.assertTrue(segmentDesc instanceof SegmentDescriptor);

    SegmentDescriptor newDesc = mapper.readValue(mapper.writeValueAsString(segmentDesc), SegmentDescriptor.class);

    Assert.assertEquals(descriptor, newDesc);
  }
}
