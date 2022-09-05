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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class TimestampMinMaxAggregatorFactoryTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    TimestampMaxAggregatorFactory maxAgg = new TimestampMaxAggregatorFactory("timeMax", "__time", null);
    TimestampMinAggregatorFactory minAgg = new TimestampMinAggregatorFactory("timeMin", "__time", null);

    Assert.assertEquals(
        maxAgg,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(maxAgg), TimestampMaxAggregatorFactory.class)
    );
    Assert.assertEquals(
        maxAgg.getCombiningFactory(),
        JSON_MAPPER.readValue(
            JSON_MAPPER.writeValueAsString(maxAgg.getCombiningFactory()),
            TimestampMaxAggregatorFactory.class
        )
    );

    Assert.assertEquals(
        minAgg,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(minAgg), TimestampMinAggregatorFactory.class)
    );
    Assert.assertEquals(
        minAgg.getCombiningFactory(),
        JSON_MAPPER.readValue(
            JSON_MAPPER.writeValueAsString(minAgg.getCombiningFactory()),
            TimestampMinAggregatorFactory.class
        )
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(TimestampMinAggregatorFactory.class)
                  .withNonnullFields("name", "comparator", "initValue")
                  .withIgnoredFields("timestampSpec")
                  .usingGetClass()
                  .verify();
    EqualsVerifier.forClass(TimestampMaxAggregatorFactory.class)
                  .withNonnullFields("name", "comparator", "initValue")
                  .withIgnoredFields("timestampSpec")
                  .usingGetClass()
                  .verify();
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
                  new TimestampMaxAggregatorFactory("timeMax", "__time", null),
                  new TimestampMinAggregatorFactory("timeMin", "__time", null)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("timeMax-access", "timeMax"),
                  new FinalizingFieldAccessPostAggregator("timeMax-finalize", "timeMax"),
                  new FieldAccessPostAggregator("timeMin-access", "timeMin"),
                  new FinalizingFieldAccessPostAggregator("timeMin-finalize", "timeMin")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("timeMax", null)
                    .add("timeMin", null)
                    .add("timeMax-access", ColumnType.LONG)
                    .add("timeMax-finalize", TimestampAggregatorFactory.FINALIZED_TYPE)
                    .add("timeMin-access", ColumnType.LONG)
                    .add("timeMin-finalize", TimestampAggregatorFactory.FINALIZED_TYPE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithName()
  {
    TimestampMaxAggregatorFactory maxAgg = new TimestampMaxAggregatorFactory("timeMax", "__time", null);
    Assert.assertEquals(maxAgg, maxAgg.withName("timeMax"));
    Assert.assertEquals("newTest", maxAgg.withName("newTest").getName());

    TimestampMinAggregatorFactory minAgg = new TimestampMinAggregatorFactory("timeMin", "__time", null);
    Assert.assertEquals(minAgg, minAgg.withName("timeMin"));
    Assert.assertEquals("newTest", minAgg.withName("newTest").getName());
  }
}
