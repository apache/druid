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

package org.apache.druid.query.aggregation.momentsketch.aggregator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
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

public class MomentSketchAggregatorFactoryTest
{
  @Test
  public void serializeDeserializeFactoryWithFieldName() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    MomentSketchAggregatorFactory factory = new MomentSketchAggregatorFactory(
        "name", "fieldName", 128, true
    );

    MomentSketchAggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        MomentSketchAggregatorFactory.class
    );

    Assert.assertEquals(factory, other);
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
                  new CountAggregatorFactory("count"),
                  new MomentSketchAggregatorFactory("moment", "col", null, null),
                  new MomentSketchMergeAggregatorFactory("momentMerge", null, null)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("moment-access", "moment"),
                  new FinalizingFieldAccessPostAggregator("moment-finalize", "moment"),
                  new FieldAccessPostAggregator("momentMerge-access", "momentMerge"),
                  new FinalizingFieldAccessPostAggregator("momentMerge-finalize", "momentMerge")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("moment", MomentSketchAggregatorFactory.TYPE)
                    .add("momentMerge", MomentSketchAggregatorFactory.TYPE)
                    .add("moment-access", MomentSketchAggregatorFactory.TYPE)
                    .add("moment-finalize", MomentSketchAggregatorFactory.TYPE)
                    .add("momentMerge-access", MomentSketchAggregatorFactory.TYPE)
                    .add("momentMerge-finalize", MomentSketchAggregatorFactory.TYPE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithName()
  {
    MomentSketchAggregatorFactory sketchAggFactory = new MomentSketchAggregatorFactory(
        "name", "fieldName", 128, true
    );
    Assert.assertEquals(sketchAggFactory, sketchAggFactory.withName("name"));
    Assert.assertEquals("newTest", sketchAggFactory.withName("newTest").getName());


    MomentSketchMergeAggregatorFactory sketchMergeAggregatorFactory = new MomentSketchMergeAggregatorFactory(
        "name", 128, true
    );
    Assert.assertEquals(sketchMergeAggregatorFactory, sketchMergeAggregatorFactory.withName("name"));
    Assert.assertEquals("newTest", sketchMergeAggregatorFactory.withName("newTest").getName());
  }
}
