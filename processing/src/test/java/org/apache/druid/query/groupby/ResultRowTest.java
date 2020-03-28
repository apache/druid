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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class ResultRowTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ResultRow row = ResultRow.of(1, 2, 3);
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Assert.assertEquals(row, objectMapper.readValue("[1, 2, 3]", ResultRow.class));
    Assert.assertEquals(row, objectMapper.readValue(objectMapper.writeValueAsBytes(row), ResultRow.class));
  }

  @Test
  public void testMapBasedRowWithNullValues()
  {
    GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        null,
        null,
        Granularities.ALL,
        ImmutableList.of(
            new DefaultDimensionSpec("dim1", "dim1"),
            new DefaultDimensionSpec("dim2", "dim2"),
            new DefaultDimensionSpec("dim3", "dim3")
        ),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    final ResultRow row = ResultRow.of("1", "2", null);
    MapBasedRow mapBasedRow = row.toMapBasedRow(query);

    // Let's make sure values are there as expected
    Assert.assertEquals("1", mapBasedRow.getRaw("dim1"));
    Assert.assertEquals("2", mapBasedRow.getRaw("dim2"));
    Assert.assertNull(mapBasedRow.getRaw("dim3"));

    // Also, let's make sure that the dimension with null value is actually present in the map
    Assert.assertTrue(mapBasedRow.getEvent().containsKey("dim3"));
  }

}
