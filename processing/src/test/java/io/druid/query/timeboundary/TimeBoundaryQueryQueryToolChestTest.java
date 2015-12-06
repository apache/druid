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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class TimeBoundaryQueryQueryToolChestTest
{
  @Test
  public void testFilterSegments() throws Exception
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        null,
        Arrays.asList(
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01/P1D");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T01/PT1H");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T02/PT1H");
              }
            }
        )
    );

    Assert.assertEquals(segments.size(), 3);

    List<LogicalSegment> expected = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T02/PT1H");
          }
        }
    );

    for (int i = 0; i < segments.size(); i++) {
       Assert.assertEquals(segments.get(i).getInterval(), expected.get(i).getInterval());
    }
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery> strategy =
        new TimeBoundaryQueryQueryToolChest().getCacheStrategy(
            new TimeBoundaryQuery(
                new TableDataSource("dummy"),
                new MultipleIntervalSegmentSpec(
                    ImmutableList.of(
                        new Interval(
                            "2015-01-01/2015-01-02"
                        )
                    )
                ),
                null,
                null
            )
        );

    final Result<TimeBoundaryResultValue> result = new Result<>(
        new DateTime(123L), new TimeBoundaryResultValue(
        ImmutableMap.of(
            TimeBoundaryQuery.MIN_TIME, new DateTime(0L).toString(),
            TimeBoundaryQuery.MAX_TIME, new DateTime("2015-01-01").toString()
        )
    )
    );

    Object preparedValue = strategy.prepareForCache().apply(
        result
    );

    ObjectMapper objectMapper = new DefaultObjectMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeBoundaryResultValue> fromCacheResult = strategy.pullFromCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }
}
