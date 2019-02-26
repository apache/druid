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

package org.apache.druid.query.movingaverage;

import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.movingaverage.averagers.DoubleMeanAveragerFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


/**
 * Unit tests for PostAveragerCalcualtor
 */
public class PostAveragerAggregatorCalculatorTest
{

  private MovingAverageQuery query;
  private PostAveragerAggregatorCalculator pac;
  private Map<String, Object> event;
  private MapBasedRow row;

  @Before
  public void setup()
  {
    System.setProperty("druid.generic.useDefaultValueForNull", "true");
    query = new MovingAverageQuery(
        new TableDataSource("d"),
        new MultipleIntervalSegmentSpec(Collections.singletonList(new Interval("2017-01-01/2017-01-01", ISOChronology.getInstanceUTC()))),
        null,
        Granularities.DAY,
        null,
        Collections.singletonList(new CountAggregatorFactory("count")),
        Collections.emptyList(),
        null,
        Collections.singletonList(new DoubleMeanAveragerFactory("avgCount", 7, 1, "count")),
        Collections.singletonList(new ArithmeticPostAggregator(
            "avgCountRatio",
            "/",
            Arrays.asList(
                new FieldAccessPostAggregator("count", "count"),
                new FieldAccessPostAggregator("avgCount", "avgCount")
            )
        )),
        null,
        null
    );

    pac = new PostAveragerAggregatorCalculator(query);
    event = new HashMap<>();
    row = new MapBasedRow(new DateTime(ISOChronology.getInstanceUTC()), event);
  }

  @Test
  public void testApply()
  {
    event.put("count", new Double(10.0));
    event.put("avgCount", new Double(12.0));

    Row result = pac.apply(row);

    assertEquals(result.getMetric("avgCountRatio").floatValue(), 10.0f / 12.0f, 0.0);
  }

  @Test
  public void testApplyMissingColumn()
  {
    event.put("count", new Double(10.0));

    Row result = pac.apply(row);

    assertEquals(result.getMetric("avgCountRatio").floatValue(), 0.0, 0.0);
    assertNull(result.getRaw("avgCountRatio"));
  }
}
