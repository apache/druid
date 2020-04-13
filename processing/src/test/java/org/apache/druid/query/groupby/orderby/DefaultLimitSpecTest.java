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

package org.apache.druid.query.groupby.orderby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class DefaultLimitSpecTest
{
  private final List<ResultRow> testRowsList;
  private final List<ResultRow> testRowsWithTimestampList;

  public DefaultLimitSpecTest()
  {
    testRowsList = ImmutableList.of(
        ResultRow.of(10.0, 1L, 2L),
        ResultRow.of(20.0, 3L, 1L),
        ResultRow.of(9.0, 2L, 3L)
    );

    testRowsWithTimestampList = ImmutableList.of(
        ResultRow.of(DateTimes.of("2011-04-01").getMillis(), 10.0, 1L, 2L),
        ResultRow.of(DateTimes.of("2011-04-01").getMillis(), 20.0, 3L, 1L),
        ResultRow.of(DateTimes.of("2011-04-01").getMillis(), 9.0, 2L, 3L)
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    //defaults
    String json = "{\"type\": \"default\"}";

    DefaultLimitSpec spec = (DefaultLimitSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );

    Assert.assertEquals(
        new DefaultLimitSpec(null, null),
        spec
    );

    //non-defaults
    json = "{\n"
           + "  \"type\":\"default\",\n"
           + "  \"columns\":[{\"dimension\":\"d\",\"direction\":\"DESCENDING\", \"dimensionOrder\":\"numeric\"}],\n"
           + "  \"limit\":10\n"
           + "}";
    spec = (DefaultLimitSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );
    Assert.assertEquals(
        new DefaultLimitSpec(ImmutableList.of(new OrderByColumnSpec("d", OrderByColumnSpec.Direction.DESCENDING,
                                                                    StringComparators.NUMERIC
        )), 10),
        spec
    );

    json = "{\n"
           + "  \"type\":\"default\",\n"
           + "  \"columns\":[{\"dimension\":\"d\",\"direction\":\"DES\", \"dimensionOrder\":\"numeric\"}],\n"
           + "  \"limit\":10\n"
           + "}";

    spec = (DefaultLimitSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );

    Assert.assertEquals(
        new DefaultLimitSpec(ImmutableList.of(new OrderByColumnSpec("d", OrderByColumnSpec.Direction.DESCENDING,
                                                                    StringComparators.NUMERIC
        )), 10),
        spec
    );

    json = "{\n"
           + "  \"type\":\"default\",\n"
           + "  \"columns\":[{\"dimension\":\"d\"}],\n"
           + "  \"limit\":10\n"
           + "}";
    spec = (DefaultLimitSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );
    Assert.assertEquals(
        new DefaultLimitSpec(ImmutableList.of(new OrderByColumnSpec("d", OrderByColumnSpec.Direction.ASCENDING,
                                                                    StringComparators.LEXICOGRAPHIC
        )), 10),
        spec
    );

    json = "{\n"
           + "  \"type\":\"default\",\n"
           + "  \"columns\":[\"d\"],\n"
           + "  \"limit\":10\n"
           + "}";
    spec = (DefaultLimitSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );
    Assert.assertEquals(
        new DefaultLimitSpec(ImmutableList.of(new OrderByColumnSpec("d", OrderByColumnSpec.Direction.ASCENDING,
                                                                    StringComparators.LEXICOGRAPHIC
        )), 10),
        spec
    );


  }

  @Test
  public void testBuildSimple()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(),
        2
    );

    Function<Sequence<ResultRow>, Sequence<ResultRow>> limitFn = limitSpec.build(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setGranularity(Granularities.NONE)
                    .build()
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsWithTimestampList.get(0), testRowsWithTimestampList.get(1)),
        limitFn.apply(Sequences.simple(testRowsWithTimestampList)).toList()
    );
  }

  @Test
  public void testWithAllGranularity()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.ASCENDING, StringComparators.NUMERIC)),
        2
    );

    Function<Sequence<ResultRow>, Sequence<ResultRow>> limitFn = limitSpec.build(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setDimensions(new DefaultDimensionSpec("k1", "k1", ValueType.DOUBLE))
                    .setGranularity(Granularities.ALL)
                    .overrideContext(ImmutableMap.of(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, true))
                    .build()
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        limitFn.apply(Sequences.simple(testRowsList)).toList()
    );
  }

  @Test
  public void testWithSortByDimsFirst()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.ASCENDING, StringComparators.NUMERIC)),
        2
    );

    Function<Sequence<ResultRow>, Sequence<ResultRow>> limitFn = limitSpec.build(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setDimensions(new DefaultDimensionSpec("k1", "k1", ValueType.DOUBLE))
                    .setGranularity(Granularities.NONE)
                    .overrideContext(ImmutableMap.of(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, true))
                    .build()
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsWithTimestampList.get(2), testRowsWithTimestampList.get(0)),
        limitFn.apply(Sequences.simple(testRowsWithTimestampList)).toList()
    );
  }

  @Test
  public void testSortDimensionDescending()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.DESCENDING)),
        2
    );

    Function<Sequence<ResultRow>, Sequence<ResultRow>> limitFn = limitSpec.build(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setDimensions(new DefaultDimensionSpec("k1", "k1", ValueType.DOUBLE))
                    .setGranularity(Granularities.ALL)
                    .build()
    );

    // Note: This test encodes the fact that limitSpec sorts numbers like strings; we might want to change this
    // in the future.
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(2), testRowsList.get(1)),
        limitFn.apply(Sequences.simple(testRowsList)).toList()
    );
  }

  @Test
  public void testBuildWithExplicitOrder()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(
            new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.ASCENDING)
        ),
        2
    );

    Function<Sequence<ResultRow>, Sequence<ResultRow>> limitFn = limitSpec.build(
        GroupByQuery.builder()
                    .setDataSource("dummy")
                    .setInterval("1000/3000")
                    .setDimensions(new DefaultDimensionSpec("k1", "k1"))
                    .setAggregatorSpecs(new LongSumAggregatorFactory("k2", "k2"))
                    .setPostAggregatorSpecs(ImmutableList.of(new ConstantPostAggregator("k3", 1L)))
                    .setGranularity(Granularities.NONE)
                    .build()
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        limitFn.apply(Sequences.simple(testRowsList)).toList()
    );
  }
}
