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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class QueriesTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

  @Test
  public void testWithSpecificSegmentsBasic()
  {
    final ImmutableList<SegmentDescriptor> descriptors = ImmutableList.of(
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
    );

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("foo")
              .intervals(
                  new MultipleSpecificSegmentSpec(
                      ImmutableList.of(
                          new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
                          new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
                      )
                  )
              )
              .granularity(Granularities.ALL)
              .build(),
        Queries.withSpecificSegments(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("foo")
                  .intervals("2000/3000")
                  .granularity(Granularities.ALL)
                  .build(),
            descriptors
        )
    );
  }

  @Test
  public void testWithSpecificSegmentsSubQueryStack()
  {
    final ImmutableList<SegmentDescriptor> descriptors = ImmutableList.of(
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
    );

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(
                  new QueryDataSource(
                      Druids.newTimeseriesQueryBuilder()
                            .dataSource(
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                          .dataSource("foo")
                                          .intervals(new MultipleSpecificSegmentSpec(descriptors))
                                          .granularity(Granularities.ALL)
                                          .build()
                                )
                            )
                            .intervals("2000/3000")
                            .granularity(Granularities.ALL)
                            .build()
                  )
              )
              .intervals("2000/3000")
              .granularity(Granularities.ALL)
              .build(),
        Queries.withSpecificSegments(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      new QueryDataSource(
                          Druids.newTimeseriesQueryBuilder()
                                .dataSource(
                                    new QueryDataSource(
                                        Druids.newTimeseriesQueryBuilder()
                                              .dataSource("foo")
                                              .intervals("2000/3000")
                                              .granularity(Granularities.ALL)
                                              .build()
                                    )
                                )
                                .intervals("2000/3000")
                                .granularity(Granularities.ALL)
                                .build()
                      )
                  )
                  .intervals("2000/3000")
                  .granularity(Granularities.ALL)
                  .build(),
            descriptors
        )
    );
  }

  @Test
  public void testWithSpecificSegmentsOnUnionIsAnError()
  {
    final ImmutableList<SegmentDescriptor> descriptors = ImmutableList.of(
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
    );

    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(new LookupDataSource("lookyloo"))
              .intervals("2000/3000")
              .granularity(Granularities.ALL)
              .build();

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Unable to apply specific segments to non-table-based dataSource");

    final Query<Result<TimeseriesResultValue>> ignored = Queries.withSpecificSegments(query, descriptors);
  }
}
