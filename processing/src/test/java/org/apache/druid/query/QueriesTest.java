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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class QueriesTest
{
  @Test
  public void testVerifyAggregations()
  {
    List<AggregatorFactory> aggFactories = Arrays.asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Collections.singletonList(
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.asList(
                new FieldAccessPostAggregator("idx", "idx"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertFalse(exceptionOccured);
  }

  @Test
  public void testVerifyAggregationsMissingVal()
  {
    List<AggregatorFactory> aggFactories = Arrays.asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Collections.singletonList(
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.asList(
                new FieldAccessPostAggregator("idx", "idx2"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertTrue(exceptionOccured);
  }

  @Test
  public void testVerifyAggregationsMultiLevel()
  {
    List<AggregatorFactory> aggFactories = Arrays.asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Arrays.asList(
        new ArithmeticPostAggregator(
            "divideStuff",
            "/",
            Arrays.asList(
                new ArithmeticPostAggregator(
                    "addStuff",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new ConstantPostAggregator("const", 1)
                    )
                ),
                new ArithmeticPostAggregator(
                    "subtractStuff",
                    "-",
                    Arrays.asList(
                        new FieldAccessPostAggregator("rev", "rev"),
                        new ConstantPostAggregator("const", 1)
                    )
                )
            )
        ),
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.asList(
                new FieldAccessPostAggregator("divideStuff", "divideStuff"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertFalse(exceptionOccured);
  }

  @Test
  public void testVerifyAggregationsMultiLevelMissingVal()
  {
    List<AggregatorFactory> aggFactories = Arrays.asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Arrays.asList(
        new ArithmeticPostAggregator(
            "divideStuff",
            "/",
            Arrays.asList(
                new ArithmeticPostAggregator(
                    "addStuff",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new ConstantPostAggregator("const", 1)
                    )
                ),
                new ArithmeticPostAggregator(
                    "subtractStuff",
                    "-",
                    Arrays.asList(
                        new FieldAccessPostAggregator("rev", "rev2"),
                        new ConstantPostAggregator("const", 1)
                    )
                )
            )
        ),
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.asList(
                new FieldAccessPostAggregator("divideStuff", "divideStuff"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertTrue(exceptionOccured);
  }
}
