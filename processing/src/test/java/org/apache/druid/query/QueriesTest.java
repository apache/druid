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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.planning.ExecutionVertexTest;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.join.JoinType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
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

    boolean exceptionOccurred = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccurred = true;
    }

    Assertions.assertFalse(exceptionOccurred);
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

    boolean exceptionOccurred = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccurred = true;
    }

    Assertions.assertTrue(exceptionOccurred);
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

    boolean exceptionOccurred = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccurred = true;
    }

    Assertions.assertFalse(exceptionOccurred);
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

    boolean exceptionOccurred = false;

    try {
      Queries.prepareAggregations(ImmutableList.of(), aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccurred = true;
    }

    Assertions.assertTrue(exceptionOccurred);
  }

  @Test
  public void testWithSpecificSegmentsBasic()
  {
    final ImmutableList<SegmentDescriptor> descriptors = ImmutableList.of(
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 0),
        new SegmentDescriptor(Intervals.of("2000/3000"), "0", 1)
    );

    Assertions.assertEquals(
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

    Assertions.assertEquals(
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

    DruidException e = assertThrows(
        DruidException.class,
        () -> Queries.withSpecificSegments(query, descriptors)
    );
    Assertions.assertEquals("Base dataSource[LookupDataSource{lookupName='lookyloo'}] is not a table!", e.getMessage());
  }

  @Test
  public void testNarrowToQueryCoveringQuery()
  {
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.of("2000/2001"), "0", 0);
    Assertions.assertSame(descriptor, Queries.narrowToQuery(descriptor, makeTimeseriesQuery("1999/2001", "2005/2006")));
  }

  @Test
  public void testNarrowToQueryPartialOverlap()
  {
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.of("2000/2001"), "0", 1);
    Assertions.assertEquals(
        new SegmentDescriptor(Intervals.of("2000-03/2001"), "0", 1),
        Queries.narrowToQuery(descriptor, makeTimeseriesQuery("1998/1999", "2000-03/2002"))
    );
  }

  @Test
  public void testNarrowToQueryNoOverlap()
  {
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.of("2000/2001"), "0", 0);
    Assertions.assertNull(Queries.narrowToQuery(descriptor, makeTimeseriesQuery("1999/2000", "2001/2002")));
  }

  @Test
  public void testNarrowToQuerySplitIsAnError()
  {
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.of("2000/2001"), "0", 0);
    final TimeseriesQuery query = makeTimeseriesQuery("1999/2000-03", "2000-06/2002");

    DruidException e = assertThrows(DruidException.class, () -> Queries.narrowToQuery(descriptor, query));
    Assertions.assertEquals(
        "Query intervals[[1999-01-01T00:00:00.000Z/2000-03-01T00:00:00.000Z, "
        + "2000-06-01T00:00:00.000Z/2002-01-01T00:00:00.000Z]] split segment interval"
        + "[2000-01-01T00:00:00.000Z/2001-01-01T00:00:00.000Z]",
        e.getMessage()
    );
  }

  @Test
  public void testNarrowToQuerySubQueryStack()
  {
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.of("2000/2001"), "0", 0);
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(new QueryDataSource(makeTimeseriesQuery("2000-03/2002")))
              .intervals("1999/2000-02")
              .granularity(Granularities.ALL)
              .build();

    Assertions.assertEquals(
        new SegmentDescriptor(Intervals.of("2000-03/2001"), "0", 0),
        Queries.narrowToQuery(descriptor, query)
    );
  }

  private static TimeseriesQuery makeTimeseriesQuery(final String... intervals)
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("foo")
                 .intervals(
                     new MultipleIntervalSegmentSpec(Arrays.stream(intervals).map(Intervals::of).toList())
                 )
                 .granularity(Granularities.ALL)
                 .build();
  }

  @Test
  public void testWithBaseDataSourceBasic()
  {
    Assertions.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("bar")
              .intervals("2000/3000")
              .granularity(Granularities.ALL)
              .build(),
        Queries.withBaseDataSource(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("foo")
                  .intervals("2000/3000")
                  .granularity(Granularities.ALL)
                  .build(),
            new TableDataSource("bar")
        )
    );
  }

  @Test
  public void testWithBaseDataSourceSubQueryStackWithJoinOnUnion()
  {
    Assertions.assertEquals(
        Druids.newTimeseriesQueryBuilder()
            .dataSource(
                JoinDataSource.create(
                    new TableDataSource("foo"),
                    ExecutionVertexTest.INLINE,
                    "j0.",
                    "\"foo.x\" == \"bar.x\"",
                    JoinType.INNER,
                    null,
                    ExprMacroTable.nil(),
                    null,
                    JoinAlgorithm.BROADCAST
                )
            )
            .intervals("2000/3000")
            .granularity(Granularities.ALL)
            .build(),
        Queries.withBaseDataSource(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    JoinDataSource.create(
                        new UnionDataSource(
                            ImmutableList.of(
                                new TableDataSource("foo"),
                                new TableDataSource("bar")
                            )
                        ),
                        ExecutionVertexTest.INLINE,
                        "j0.",
                        "\"foo.x\" == \"bar.x\"",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        null,
                        JoinAlgorithm.BROADCAST
                    )
                )
                .intervals("2000/3000")
                .granularity(Granularities.ALL)
                .build(),
            new TableDataSource("foo")
        )
    );
  }

  @Test
  public void testWithBaseDataSourcedBaseFilterWithMultiJoin()
  {
    Assertions.assertEquals(
        Druids.newTimeseriesQueryBuilder()
            .dataSource(
                JoinDataSource.create(
                    JoinDataSource.create(
                        new TableDataSource("foo"),
                        ExecutionVertexTest.INLINE,
                        "j1.",
                        "\"foo.x\" == \"bar.x\"",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        null,
                        JoinAlgorithm.BROADCAST

                    ),
                    ExecutionVertexTest.INLINE,
                    "j0.",
                    "\"foo_outer.x\" == \"bar.x\"",
                    JoinType.INNER,
                    null,
                    ExprMacroTable.nil(),
                    null,
                    JoinAlgorithm.BROADCAST
                )
            )
            .intervals("2000/3000")
            .granularity(Granularities.ALL)
            .build(),
        Queries.withBaseDataSource(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    JoinDataSource.create(
                        JoinDataSource.create(
                            new TableDataSource("foo_inner"),
                            ExecutionVertexTest.INLINE,
                            "j1.",
                            "\"foo.x\" == \"bar.x\"",
                            JoinType.INNER,
                            TrueDimFilter.instance(),
                            ExprMacroTable.nil(),
                            null,
                            JoinAlgorithm.BROADCAST

                        ),
                        ExecutionVertexTest.INLINE,
                        "j0.",
                        "\"foo_outer.x\" == \"bar.x\"",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        null,
                        JoinAlgorithm.BROADCAST

                    )

                )
                .intervals("2000/3000")
                .granularity(Granularities.ALL)
                .build(),
            new TableDataSource("foo")
        )
    );
  }

  @Test
  public void testWithBaseDataSourcedIsRejectedForSubQuery()
  {
    DruidException e = assertThrows(
        DruidException.class,
        () -> Queries.withBaseDataSource(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    new QueryDataSource(
                        Druids.newTimeseriesQueryBuilder()
                            .dataSource(
                                JoinDataSource.create(
                                    JoinDataSource.create(
                                        new TableDataSource("foo_inner"),
                                        new TableDataSource("bar"),
                                        "j1.",
                                        "\"foo.x\" == \"bar.x\"",
                                        JoinType.INNER,
                                        TrueDimFilter.instance(),
                                        ExprMacroTable.nil(),
                                        null,
                                        JoinAlgorithm.BROADCAST

                                    ),
                                    new TableDataSource("foo_outer"),
                                    "j0.",
                                    "\"foo_outer.x\" == \"bar.x\"",
                                    JoinType.INNER,
                                    null,
                                    ExprMacroTable.nil(),
                                    null,
                                    JoinAlgorithm.BROADCAST

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
            new TableDataSource("foo")
        )
    );
    DruidExceptionMatcher.assertThat(
        e,
        DruidExceptionMatcher.defensive()
            .expectMessageContains("Its unsafe to replace the BaseDataSource of a non-processable query")
    );
  }

  @Test
  public void testWithMaxScatterGatherBytesAppliesLimitWhenExplicitlyNull()
  {
    final Map<String, Object> context = new HashMap<>();
    context.put(QueryContextParameters.MAX_SCATTER_GATHER_BYTES.getName(), null);
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("foo")
                                        .intervals("2000/3000")
                                        .granularity(Granularities.ALL)
                                        .context(context)
                                        .build();

    Assertions.assertEquals(
        100L,
        Queries.withMaxScatterGatherBytes(query, 100L).context().get(QueryContextParameters.MAX_SCATTER_GATHER_BYTES)
    );
  }
}
