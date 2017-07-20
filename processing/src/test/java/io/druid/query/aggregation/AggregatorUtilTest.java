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

package io.druid.query.aggregation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.Pair;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.druid.query.QueryRunnerTestHelper.dependentPostAggMetric;

public class AggregatorUtilTest
{

  @Test
  public void testPruneDependentPostAgg()
  {
    PostAggregator agg1 = new ArithmeticPostAggregator(
        "abc", "+", Lists.<PostAggregator>newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("2", 2L)
    )
    );
    PostAggregator dependency1 = new ArithmeticPostAggregator(
        "dep1", "+", Lists.<PostAggregator>newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("4", 4L)
    )
    );
    PostAggregator agg2 = new FieldAccessPostAggregator("def", "def");
    PostAggregator dependency2 = new FieldAccessPostAggregator("dep2", "dep2");
    PostAggregator aggregator = new ArithmeticPostAggregator(
        "finalAgg",
        "+",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator("dep1", "dep1"),
            new FieldAccessPostAggregator("dep2", "dep2")
        )
    );
    List<PostAggregator> prunedAgg = AggregatorUtil.pruneDependentPostAgg(
        Lists.newArrayList(
            agg1,
            dependency1,
            agg2,
            dependency2,
            aggregator
        ), aggregator.getName()
    );
    Assert.assertEquals(Lists.newArrayList(dependency1, dependency2, aggregator), prunedAgg);
  }

  @Test
  public void testOutOfOrderPruneDependentPostAgg()
  {
    PostAggregator agg1 = new ArithmeticPostAggregator(
        "abc", "+", Lists.<PostAggregator>newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("2", 2L)
    )
    );
    PostAggregator dependency1 = new ArithmeticPostAggregator(
        "dep1", "+", Lists.<PostAggregator>newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("4", 4L)
    )
    );
    PostAggregator agg2 = new FieldAccessPostAggregator("def", "def");
    PostAggregator dependency2 = new FieldAccessPostAggregator("dep2", "dep2");
    PostAggregator aggregator = new ArithmeticPostAggregator(
        "finalAgg",
        "+",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator("dep1", "dep1"),
            new FieldAccessPostAggregator("dep2", "dep2")
        )
    );
    List<PostAggregator> prunedAgg = AggregatorUtil.pruneDependentPostAgg(
        Lists.newArrayList(
            agg1,
            dependency1,
            aggregator, // dependency is added later than the aggregator
            agg2,
            dependency2
        ), aggregator.getName()
    );
    Assert.assertEquals(Lists.newArrayList(dependency1, aggregator), prunedAgg);
  }

  @Test
  public void testCondenseAggregators()
  {

    ArrayList<AggregatorFactory> aggregatorFactories = Lists.<AggregatorFactory>newArrayList(
        Iterables.concat(
            QueryRunnerTestHelper.commonDoubleAggregators,
            Lists.newArrayList(
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
    );

    List<PostAggregator> postAggregatorList = Arrays.<PostAggregator>asList(
        QueryRunnerTestHelper.addRowsIndexConstant,
        QueryRunnerTestHelper.dependentPostAgg
    );
    Pair<List<AggregatorFactory>, List<PostAggregator>> aggregatorsPair = AggregatorUtil.condensedAggregators(
        aggregatorFactories,
        postAggregatorList,
        dependentPostAggMetric
    );
    // verify aggregators
    Assert.assertEquals(
        Lists.newArrayList(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum),
        aggregatorsPair.lhs
    );
    Assert.assertEquals(
        Lists.newArrayList(
            QueryRunnerTestHelper.addRowsIndexConstant,
            QueryRunnerTestHelper.dependentPostAgg
        ), aggregatorsPair.rhs
    );

  }

  @Test
  public void testNullPostAggregatorNames()
  {
    AggregatorFactory agg1 = new DoubleSumAggregatorFactory("agg1", "value");
    AggregatorFactory agg2 = new DoubleSumAggregatorFactory("agg2", "count");
    PostAggregator postAgg1 = new ArithmeticPostAggregator(
        null, "*", Lists.<PostAggregator>newArrayList(
        new FieldAccessPostAggregator(
            null,
            "agg1"
        ), new FieldAccessPostAggregator(null, "agg2")
    )
    );

    PostAggregator postAgg2 = new ArithmeticPostAggregator(
        "postAgg",
        "/",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator(
                null,
                "agg1"
            ), new FieldAccessPostAggregator(null, "agg2")
        )
    );

    Assert.assertEquals(
        new Pair(Lists.newArrayList(agg1, agg2), Lists.newArrayList(postAgg2)), AggregatorUtil.condensedAggregators(
        Lists.newArrayList(agg1, agg2),
        Lists.newArrayList(postAgg1, postAgg2),
        "postAgg"
    )
    );

  }

  @Test
  public void testCasing()
  {
    AggregatorFactory agg1 = new DoubleSumAggregatorFactory("Agg1", "value");
    AggregatorFactory agg2 = new DoubleSumAggregatorFactory("Agg2", "count");
    PostAggregator postAgg1 = new ArithmeticPostAggregator(
        null, "*", Lists.<PostAggregator>newArrayList(
        new FieldAccessPostAggregator(
            null,
            "Agg1"
        ), new FieldAccessPostAggregator(null, "Agg2")
    )
    );

    PostAggregator postAgg2 = new ArithmeticPostAggregator(
        "postAgg",
        "/",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator(
                null,
                "Agg1"
            ), new FieldAccessPostAggregator(null, "Agg2")
        )
    );

    Assert.assertEquals(
        new Pair(Lists.newArrayList(agg1, agg2), Lists.newArrayList(postAgg2)), AggregatorUtil.condensedAggregators(
        Lists.newArrayList(agg1, agg2),
        Lists.newArrayList(postAgg1, postAgg2),
        "postAgg"
    )
    );
  }

}
