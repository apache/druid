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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
    ObjectMapper mapper = TestHelper.getObjectMapper();

    //defaults
    String json = "{\"type\": \"default\"}";

    DefaultLimitSpec spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, DefaultLimitSpec.class)),
        DefaultLimitSpec.class
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
    spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, DefaultLimitSpec.class)),
        DefaultLimitSpec.class
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

    spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, DefaultLimitSpec.class)),
        DefaultLimitSpec.class
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
    spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, DefaultLimitSpec.class)),
        DefaultLimitSpec.class
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
    spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, DefaultLimitSpec.class)),
        DefaultLimitSpec.class
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
        ImmutableList.<OrderByColumnSpec>of(),
        2
    );

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        ImmutableList.<DimensionSpec>of(),
        ImmutableList.<AggregatorFactory>of(),
        ImmutableList.<PostAggregator>of()
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
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
        ImmutableList.<DimensionSpec>of(new DefaultDimensionSpec("k1", "k1")),
        ImmutableList.<AggregatorFactory>of(),
        ImmutableList.<PostAggregator>of()
    );

    // Note: This test encodes the fact that limitSpec sorts numbers like strings; we might want to change this
    // in the future.
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(2), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
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
        ImmutableList.<DimensionSpec>of(
            new DefaultDimensionSpec("k1", "k1")
        ),
        ImmutableList.<AggregatorFactory>of(
            new LongSumAggregatorFactory("k2", "k2")
        ),
        ImmutableList.<PostAggregator>of(
            new ConstantPostAggregator("k3", 1L)
        )
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );

    // if there is an aggregator with same name then that is used to build ordering
    limitFn = limitSpec.build(
        ImmutableList.<DimensionSpec>of(
            new DefaultDimensionSpec("k1", "k1")
        ),
        ImmutableList.<AggregatorFactory>of(
            new LongSumAggregatorFactory("k1", "k1")
        ),
        ImmutableList.<PostAggregator>of(
            new ConstantPostAggregator("k3", 1L)
        )
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );

    // if there is a post-aggregator with same name then that is used to build ordering
    limitFn = limitSpec.build(
        ImmutableList.<DimensionSpec>of(
            new DefaultDimensionSpec("k1", "k1")
        ),
        ImmutableList.<AggregatorFactory>of(
            new LongSumAggregatorFactory("k2", "k2")
        ),
        ImmutableList.<PostAggregator>of(
            new ArithmeticPostAggregator(
                "k1",
                "+",
                ImmutableList.<PostAggregator>of(
                    new ConstantPostAggregator("x", 1),
                    new ConstantPostAggregator("y", 1))
            )
        )
    );
    Assert.assertEquals(
        (List)ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        (List)Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );

    // makes same result
    limitFn = limitSpec.build(
        ImmutableList.<DimensionSpec>of(new DefaultDimensionSpec("k1", "k1")),
        ImmutableList.<AggregatorFactory>of(new LongSumAggregatorFactory("k2", "k2")),
        ImmutableList.<PostAggregator>of(new ExpressionPostAggregator("k1", "1 + 1"))
    );
    Assert.assertEquals(
        (List)ImmutableList.of(testRowsList.get(2), testRowsList.get(0)),
        (List)Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );
  }

  private Row createRow(String timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    DateTime ts = new DateTime(timestamp);
    return new MapBasedRow(ts, theVals);
  }
}
