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

package org.apache.druid.msq.querykit;

import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.FilteredSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link BaseInputSegmentMapFunction}.
 */
public class BaseInputSegmentMapFunctionTest
{
  private static final SelectorDimFilter FILTER = new SelectorDimFilter("dim1", "abc", null);
  private static final InlineDataSource INLINE = InlineDataSource.fromIterable(
      List.<Object[]>of(new Object[]{"abc"}),
      RowSignature.builder().add("dim1", ColumnType.STRING).build()
  );

  @Test
  public void test_createAll_singleInput()
  {
    final List<BaseInputSegmentMapFunction> fns = createAll(new InputNumberDataSource(3));

    Assertions.assertEquals(List.of(3), inputNumbers(fns));
    Assertions.assertFalse(apply(fns.get(0)) instanceof FilteredSegment);
  }

  @Test
  public void test_createAll_restrictedInput()
  {
    final List<BaseInputSegmentMapFunction> fns =
        createAll(new RestrictedInputNumberDataSource(1, NoRestrictionPolicy.instance()));

    Assertions.assertEquals(List.of(1), inputNumbers(fns));
  }

  @Test
  public void test_createAll_unionArmsNumberedOutOfOrder()
  {
    // Each arm's function carries the input number the arm itself reads, not its position in the union.
    final List<BaseInputSegmentMapFunction> fns = createAll(
        new UnionDataSource(
            List.of(
                FilteredDataSource.create(new InputNumberDataSource(2), FILTER),
                new InputNumberDataSource(0)
            )
        )
    );

    Assertions.assertEquals(List.of(2, 0), inputNumbers(fns));
    Assertions.assertTrue(apply(fns.get(0)) instanceof FilteredSegment);
    Assertions.assertFalse(apply(fns.get(1)) instanceof FilteredSegment);
  }

  @Test
  public void test_createAll_filterAboveUnion()
  {
    // A datasource layered on top of the union applies to every arm.
    final List<BaseInputSegmentMapFunction> fns = createAll(
        FilteredDataSource.create(
            new UnionDataSource(List.of(new InputNumberDataSource(0), new InputNumberDataSource(1))),
            FILTER
        )
    );

    Assertions.assertEquals(List.of(0, 1), inputNumbers(fns));
    Assertions.assertTrue(apply(fns.get(0)) instanceof FilteredSegment);
    Assertions.assertTrue(apply(fns.get(1)) instanceof FilteredSegment);
  }

  @Test
  public void test_createAll_nestedUnion()
  {
    // Only the outermost union is flattened, so an arm that is itself a union has no single input number.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> createAll(
            new UnionDataSource(
                List.of(
                    new UnionDataSource(List.of(new InputNumberDataSource(1), new InputNumberDataSource(0))),
                    new InputNumberDataSource(2)
                )
            )
        )
    );

    Assertions.assertTrue(e.getMessage().contains("does not have an associated inputNumber"), e.getMessage());
  }

  @Test
  public void test_createAll_unnumberedArm()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> createAll(new UnionDataSource(List.of(new InputNumberDataSource(0), INLINE)))
    );

    Assertions.assertTrue(e.getMessage().contains("does not have an associated inputNumber"), e.getMessage());
  }

  @Test
  public void test_createAll_unnumberedBaseFromOlderController()
  {
    // Older controllers send a sole base input as its original datasource, which reads the stage's base input.
    final List<BaseInputSegmentMapFunction> fns = BaseInputSegmentMapFunction.createAll(
        BaseInputVertex.withNumberedBase(makeQuery(INLINE), IntList.of(1)),
        NoopPolicyEnforcer.instance()
    );

    Assertions.assertEquals(List.of(1), inputNumbers(fns));
  }

  @Test
  public void test_createAll_joinOverUnionBuildsJoinablesOnce()
  {
    final AtomicInteger builds = new AtomicInteger();
    final List<BaseInputSegmentMapFunction> fns = createAll(
        joinInline(
            new UnionDataSource(
                List.of(FilteredDataSource.create(new InputNumberDataSource(0), FILTER), new InputNumberDataSource(1))
            ),
            builds
        )
    );

    Assertions.assertEquals(List.of(0, 1), inputNumbers(fns));
    Assertions.assertEquals(List.of("join->filtered->base", "join->base"), debugStrings(fns));
    Assertions.assertEquals(1, builds.get());
  }

  @Test
  public void test_createAll_filterOverJoinOverUnionBuildsJoinablesOnce()
  {
    final AtomicInteger builds = new AtomicInteger();
    final List<BaseInputSegmentMapFunction> fns =
        createAll(FilteredDataSource.create(joinInline(makeUnion(), builds), FILTER));

    Assertions.assertEquals(List.of(0, 1), inputNumbers(fns));
    Assertions.assertEquals(List.of("filtered->join->base", "filtered->join->base"), debugStrings(fns));
    Assertions.assertEquals(1, builds.get());
  }

  @Test
  public void test_createAll_joinOverFilterOverUnionBuildsJoinablesOnce()
  {
    final AtomicInteger builds = new AtomicInteger();
    final List<BaseInputSegmentMapFunction> fns =
        createAll(joinInline(FilteredDataSource.create(makeUnion(), FILTER), builds));

    Assertions.assertEquals(List.of(0, 1), inputNumbers(fns));
    Assertions.assertEquals(List.of("join->filtered->base", "join->filtered->base"), debugStrings(fns));
    Assertions.assertEquals(1, builds.get());
  }

  @Test
  public void test_createAll_collapsedSubqueryOverJoinOverUnionBuildsJoinablesOnce()
  {
    // The outer groupBy collapses into the inner one, so the join is mapped as part of the outer query's vertex.
    final AtomicInteger builds = new AtomicInteger();
    final List<BaseInputSegmentMapFunction> fns = BaseInputSegmentMapFunction.createAll(
        makeGroupByQuery(new QueryDataSource(makeGroupByQuery(joinInline(makeUnion(), builds)))),
        NoopPolicyEnforcer.instance()
    );

    Assertions.assertEquals(List.of(0, 1), inputNumbers(fns));
    Assertions.assertEquals(List.of("join->base", "join->base"), debugStrings(fns));
    Assertions.assertEquals(1, builds.get());
  }

  @Test
  public void test_createAll_inputReadTwice()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> createAll(new UnionDataSource(List.of(new InputNumberDataSource(0), new InputNumberDataSource(0))))
    );

    Assertions.assertTrue(e.getMessage().contains("Duplicate inputNumbers[[0, 0]]"), e.getMessage());
  }

  private static List<BaseInputSegmentMapFunction> createAll(final DataSource dataSource)
  {
    return BaseInputSegmentMapFunction.createAll(makeQuery(dataSource), NoopPolicyEnforcer.instance());
  }

  private static UnionDataSource makeUnion()
  {
    return new UnionDataSource(List.of(new InputNumberDataSource(0), new InputNumberDataSource(1)));
  }

  /**
   * Returns a left join of {@code left} against {@link #INLINE}, counting each joinable built for it in
   * {@code builds}.
   */
  private static JoinDataSource joinInline(final DataSource left, final AtomicInteger builds)
  {
    final JoinableFactory countingFactory = new JoinableFactory()
    {
      @Override
      public boolean isDirectlyJoinable(final DataSource dataSource)
      {
        return dataSource instanceof InlineDataSource;
      }

      @Override
      public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
      {
        builds.incrementAndGet();
        return new InlineJoinableFactory().build(dataSource, condition);
      }
    };

    return JoinDataSource.create(
        left,
        INLINE,
        "j.",
        JoinConditionAnalysis.forExpression("dim1 == \"j.dim1\"", "j.", ExprMacroTable.nil()),
        JoinType.LEFT,
        null,
        new JoinableFactoryWrapper(countingFactory),
        JoinAlgorithm.BROADCAST
    );
  }

  private static GroupByQuery makeGroupByQuery(final DataSource dataSource)
  {
    return GroupByQuery.builder()
                       .setDataSource(dataSource)
                       .setInterval(QuerySegmentSpec.ETERNITY)
                       .setGranularity(Granularities.ALL)
                       .build();
  }

  private static ScanQuery makeQuery(final DataSource dataSource)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(dataSource)
                 .intervals(QuerySegmentSpec.ETERNITY)
                 .columns("dim1")
                 .build();
  }

  private static List<Integer> inputNumbers(final List<BaseInputSegmentMapFunction> fns)
  {
    return fns.stream().map(BaseInputSegmentMapFunction::inputNumber).toList();
  }

  private static Segment apply(final BaseInputSegmentMapFunction fn)
  {
    return fn.segmentMapFn().apply(Optional.of(Mockito.mock(Segment.class))).orElseThrow();
  }

  /**
   * Returns the debug string of the segment that each function makes from a base segment whose debug string is
   * {@code base}, which shows the wrappers each function applies, outermost first.
   */
  private static List<String> debugStrings(final List<BaseInputSegmentMapFunction> fns)
  {
    final Segment segment = Mockito.mock(Segment.class);
    Mockito.when(segment.getDebugString()).thenReturn("base");
    return fns.stream()
              .map(fn -> fn.segmentMapFn().apply(Optional.of(segment)).orElseThrow().getDebugString())
              .toList();
  }
}
