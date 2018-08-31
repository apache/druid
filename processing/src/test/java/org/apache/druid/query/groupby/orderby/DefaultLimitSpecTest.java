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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 */
public class DefaultLimitSpecTest
{
  private final List<Row> testRowsList;
  private final Sequence<Row> testRowsSequence;

  public DefaultLimitSpecTest()
  {
    testRowsList = ImmutableList.of(
        createRow("2011-04-01", "k1", 10.0, "k2", 1L, "k3", 2L),
        createRow("2011-04-01", "k1", 20.0, "k2", 3L, "k3", 1L),
        createRow("2011-04-01", "k1", 9.0, "k2", 2L, "k3", 3L)
    );

    testRowsSequence = Sequences.simple(testRowsList);
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
                                                                    StringComparators.NUMERIC)), 10),
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
                                                                    StringComparators.NUMERIC)), 10),
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
                                                                    StringComparators.LEXICOGRAPHIC)), 10),
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
                                                                    StringComparators.LEXICOGRAPHIC)), 10),
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

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        Granularities.NONE,
        false
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        limitFn.apply(testRowsSequence).toList()
    );
  }

  @Test
  public void testWithAllGranularity()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.ASCENDING, StringComparators.NUMERIC)),
        2
    );

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        ImmutableList.of(new DefaultDimensionSpec("k1", "k1", ValueType.DOUBLE)),
        ImmutableList.of(),
        ImmutableList.of(),
        Granularities.ALL,
        true
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        limitFn.apply(testRowsSequence).toList()
    );
  }

  @Test
  public void testWithSortByDimsFirst()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.ASCENDING, StringComparators.NUMERIC)),
        2
    );

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        ImmutableList.of(new DefaultDimensionSpec("k1", "k1", ValueType.DOUBLE)),
        ImmutableList.of(),
        ImmutableList.of(),
        Granularities.NONE,
        true
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        limitFn.apply(testRowsSequence).toList()
    );
  }

  @Test
  public void testSortDimensionDescending()
  {
    DefaultLimitSpec limitSpec = new DefaultLimitSpec(
        ImmutableList.of(new OrderByColumnSpec("k1", OrderByColumnSpec.Direction.DESCENDING)),
        2
    );

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        ImmutableList.of(new DefaultDimensionSpec("k1", "k1")),
        ImmutableList.of(),
        ImmutableList.of(),
        Granularities.NONE,
        false
    );

    // Note: This test encodes the fact that limitSpec sorts numbers like strings; we might want to change this
    // in the future.
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(2), testRowsList.get(1)),
        limitFn.apply(testRowsSequence).toList()
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

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        ImmutableList.of(
            new DefaultDimensionSpec("k1", "k1")
        ),
        ImmutableList.of(
            new LongSumAggregatorFactory("k2", "k2")
        ),
        ImmutableList.of(
            new ConstantPostAggregator("k3", 1L)
        ),
        Granularities.NONE,
        false
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        limitFn.apply(testRowsSequence).toList()
    );

    // if there is an aggregator with same name then that is used to build ordering
    limitFn = limitSpec.build(
        ImmutableList.of(
            new DefaultDimensionSpec("k1", "k1")
        ),
        ImmutableList.of(
            new LongSumAggregatorFactory("k1", "k1")
        ),
        ImmutableList.of(
            new ConstantPostAggregator("k3", 1L)
        ),
        Granularities.NONE,
        false
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        limitFn.apply(testRowsSequence).toList()
    );

    // if there is a post-aggregator with same name then that is used to build ordering
    limitFn = limitSpec.build(
        ImmutableList.of(
            new DefaultDimensionSpec("k1", "k1")
        ),
        ImmutableList.of(
            new LongSumAggregatorFactory("k2", "k2")
        ),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "k1",
                "+",
                ImmutableList.of(
                    new ConstantPostAggregator("x", 1),
                    new ConstantPostAggregator("y", 1))
            )
        ),
        Granularities.NONE,
        false
    );
    Assert.assertEquals(
        (List) ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        (List) limitFn.apply(testRowsSequence).toList()
    );

    // makes same result
    limitFn = limitSpec.build(
        ImmutableList.of(new DefaultDimensionSpec("k1", "k1")),
        ImmutableList.of(new LongSumAggregatorFactory("k2", "k2")),
        ImmutableList.of(new ExpressionPostAggregator("k1", "1 + 1", null, TestExprMacroTable.INSTANCE)),
        Granularities.NONE,
        false
    );
    Assert.assertEquals(
        (List) ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        (List) limitFn.apply(testRowsSequence).toList()
    );
  }

  private Row createRow(String timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    return new MapBasedRow(DateTimes.of(timestamp), theVals);
  }
}
