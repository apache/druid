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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryQueryRunnerUnitTest
{

  @Test
  public void blah()
  {
    QueryRunner baseRunner = mock(QueryRunner.class);
    QueryRunner biFunctionRunner = mock(QueryRunner.class);

    when(baseRunner.run(any(), any())).thenReturn(makeBaseSequence(Arrays.asList(1, 2, 3, 4, 5)));
    when(biFunctionRunner.run(any(), any())).thenReturn(makeBaseSequence(Arrays.asList(1, 2, 3, 4, 5)));

    BiFunction<Query, List<SegmentDescriptor>, QueryRunner> biFunction = (query, segmentDescriptors) -> biFunctionRunner;

    RetryQueryRunnerConfig config = mock(RetryQueryRunnerConfig.class);
    when(config.getNumTries()).thenReturn(2);
    when(config.isReturnPartialResults()).thenReturn(true);
    RetryQueryRunner queryRunner = new RetryQueryRunner(
        baseRunner,
        biFunction,
        config, new DefaultObjectMapper()
    );

    Query<Result<TimeseriesResultValue>> query = timeseriesQuery(new Interval(0, 100));
    QueryPlus<Result<TimeseriesResultValue>> plus = QueryPlus.wrap(query).withIdentity("foo");
    ResponseContext context = new DefaultResponseContext();
    ResponseContext spyContext = spy(context);
    when(spyContext.didNodeDisconnect()).thenReturn(true).thenReturn(false);

    ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
    map.put("foo", 0);
    when(spyContext.getRemainingResponses()).thenReturn(map);

    List<SegmentDescriptor> missingSegments = new ArrayList<>();
    missingSegments.add(mock(SegmentDescriptor.class));

    when(spyContext.getMissingSegments()).thenReturn(null).thenReturn(missingSegments);
    Sequence result = queryRunner.run(plus, spyContext);
    // need to materialize the sequence and test the iterator
    List list = result.toList();
    for (Object o : list) {
      // do nothing
    }
    // base runner gets called once initially, then again to retry query
    verify(baseRunner, times(2)).run(any(), any());
    // now the retry segments gets called once
    verify(biFunctionRunner, times(1)).run(any(), any());
  }

  protected static Query<Result<TimeseriesResultValue>> timeseriesQuery(Interval interval)
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("foo")
                 .intervals(ImmutableList.of(interval))
                 .granularity(Granularities.HOUR)
                 .queryId("foo")
                 .aggregators(new CountAggregatorFactory("rows"))
                 .context(
                     ImmutableMap.of(
                         QueryContexts.RETRY_ON_DISCONNECT,
                         true
                     )
                 )
                 .build()
                 .withId("foo");
  }

  private static <T> Sequence<T> makeBaseSequence(final Iterable<T> iterable)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return iterable.iterator();
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {

          }
        }
    );
  }

}
