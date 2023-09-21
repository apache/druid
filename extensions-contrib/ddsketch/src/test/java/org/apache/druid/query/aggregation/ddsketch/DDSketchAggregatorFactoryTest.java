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

package org.apache.druid.query.aggregation.ddsketch;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class DDSketchAggregatorFactoryTest
{
  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new DDSketchAggregatorFactory("ddsketch", "col", 0.01, 1000)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("ddsketch-access", "ddsketch"),
                  new FinalizingFieldAccessPostAggregator("ddsketch-finalize", "ddsketch")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("ddsketch", DDSketchAggregatorFactory.TYPE)
                    .add("ddsketch-access", DDSketchAggregatorFactory.TYPE)
                    .add("ddsketch-finalize", DDSketchAggregatorFactory.TYPE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithName()
  {
    DDSketchAggregatorFactory factory = new DDSketchAggregatorFactory("ddsketch", "col", 0.01, 1000);
    Assert.assertEquals(factory, factory.withName("ddsketch"));
    Assert.assertEquals("newTest", factory.withName("newTest").getName());
  }
}
