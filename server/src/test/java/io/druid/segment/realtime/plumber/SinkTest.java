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

package io.druid.segment.realtime.plumber;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireHydrant;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class SinkTest
{
  @Test
  public void testSwap() throws Exception
  {
    final DataSchema schema = new DataSchema(
        "test",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, null),
        new DefaultObjectMapper()
    );

    final Interval interval = new Interval("2013-01-01/2013-01-02");
    final String version = new DateTime().toString();
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        100,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null
    );
    final Sink sink = new Sink(
        interval,
        schema,
        tuningConfig.getShardSpec(),
        version,
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.isReportParseExceptions()
    );

    sink.add(
        new InputRow()
        {
          @Override
          public List<String> getDimensions()
          {
            return Lists.newArrayList();
          }

          @Override
          public long getTimestampFromEpoch()
          {
            return new DateTime("2013-01-01").getMillis();
          }

          @Override
          public DateTime getTimestamp()
          {
            return new DateTime("2013-01-01");
          }

          @Override
          public List<String> getDimension(String dimension)
          {
            return Lists.newArrayList();
          }

          @Override
          public float getFloatMetric(String metric)
          {
            return 0;
          }

          @Override
          public long getLongMetric(String metric)
          {
            return 0L;
          }

          @Override
          public double getDoubleMetric(String metric)
          {
            return 0.0d;
          }

          @Override
          public Object getRaw(String dimension)
          {
            return null;
          }

          @Override
          public int compareTo(Row o)
          {
            return 0;
          }
        }
    );

    FireHydrant currHydrant = sink.getCurrHydrant();
    Assert.assertEquals(new Interval("2013-01-01/PT1M"), currHydrant.getIndex().getInterval());


    FireHydrant swapHydrant = sink.swap();

    sink.add(
        new InputRow()
        {
          @Override
          public List<String> getDimensions()
          {
            return Lists.newArrayList();
          }

          @Override
          public long getTimestampFromEpoch()
          {
            return new DateTime("2013-01-01").getMillis();
          }

          @Override
          public DateTime getTimestamp()
          {
            return new DateTime("2013-01-01");
          }

          @Override
          public List<String> getDimension(String dimension)
          {
            return Lists.newArrayList();
          }

          @Override
          public float getFloatMetric(String metric)
          {
            return 0;
          }

          @Override
          public long getLongMetric(String metric)
          {
            return 0L;
          }

          @Override
          public double getDoubleMetric(String metric)
          {
            return 0.0d;
          }

          @Override
          public Object getRaw(String dimension)
          {
            return null;
          }

          @Override
          public int compareTo(Row o)
          {
            return 0;
          }
        }
    );

    Assert.assertEquals(currHydrant, swapHydrant);
    Assert.assertNotSame(currHydrant, sink.getCurrHydrant());
    Assert.assertEquals(new Interval("2013-01-01/PT1M"), sink.getCurrHydrant().getIndex().getInterval());

    Assert.assertEquals(2, Iterators.size(sink.iterator()));
  }
}
