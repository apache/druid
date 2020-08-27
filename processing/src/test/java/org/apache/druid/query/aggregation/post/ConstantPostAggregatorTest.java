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

package org.apache.druid.query.aggregation.post;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class ConstantPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    ConstantPostAggregator constantPostAggregator;

    constantPostAggregator = new ConstantPostAggregator("shichi", 7);
    Assert.assertEquals(7, constantPostAggregator.compute(null));
    constantPostAggregator = new ConstantPostAggregator("rei", 0.0);
    Assert.assertEquals(0.0, constantPostAggregator.compute(null));
    constantPostAggregator = new ConstantPostAggregator("ichi", 1.0);
    Assert.assertNotSame(1, constantPostAggregator.compute(null));
  }

  @Test
  public void testComparator()
  {
    ConstantPostAggregator constantPostAggregator =
        new ConstantPostAggregator("thistestbasicallydoesnothing unhappyface", 1);
    Comparator comp = constantPostAggregator.getComparator();
    Assert.assertEquals(0, comp.compare(0, constantPostAggregator.compute(null)));
    Assert.assertEquals(0, comp.compare(0, 1));
    Assert.assertEquals(0, comp.compare(1, 0));
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ConstantPostAggregator aggregator = new ConstantPostAggregator("aggregator", 2);
    ConstantPostAggregator aggregator1 = mapper.readValue(
        mapper.writeValueAsString(aggregator),
        ConstantPostAggregator.class
    );
    Assert.assertEquals(aggregator, aggregator1);
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count")
              )
              .postAggregators(
                  new ConstantPostAggregator("a", 3L),
                  new ConstantPostAggregator("b", 1.0f),
                  new ConstantPostAggregator("c", 5.0)
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ValueType.LONG)
                    .add("a", ValueType.LONG)
                    .add("b", ValueType.DOUBLE)
                    .add("c", ValueType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
