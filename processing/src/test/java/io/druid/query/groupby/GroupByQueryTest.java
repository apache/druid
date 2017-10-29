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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.TestHelper;
import io.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class GroupByQueryTest
{
  private static final ObjectMapper jsonMapper = TestHelper.getJsonMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(new FieldAccessPostAggregator("x", "idx")))
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(new OrderByColumnSpec(
                    "alias",
                    OrderByColumnSpec.Direction.ASCENDING,
                    StringComparators.LEXICOGRAPHIC
                )),
                100
            )
        )
        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testRowOrderingMixTypes()
  {
    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("dummy")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval("2000/2001")
                                           .addDimension(new DefaultDimensionSpec("foo", "foo", ValueType.LONG))
                                           .addDimension(new DefaultDimensionSpec("bar", "bar", ValueType.FLOAT))
                                           .addDimension(new DefaultDimensionSpec("baz", "baz", ValueType.STRING))
                                           .build();

    final Ordering<Row> rowOrdering = query.getRowOrdering(false);
    final int compare = rowOrdering.compare(
        new MapBasedRow(0L, ImmutableMap.of("foo", 1, "bar", 1f, "baz", "a")),
        new MapBasedRow(0L, ImmutableMap.of("foo", 1L, "bar", 1d, "baz", "b"))
    );
    Assert.assertEquals(-1, compare);
  }
}
