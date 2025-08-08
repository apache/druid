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

package org.apache.druid.frame.segment;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.extraction.UpperExtractionFn;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FrameCursorFactoryTest
{
  /**
   * Basic tests: everything except makeCursor, makeVectorCursor.
   */
  @RunWith(Parameterized.class)
  public static class BasicTests extends InitializedNullHandlingTest
  {
    private final FrameType frameType;

    private CursorFactory queryableCursorFactory;
    private FrameSegment frameSegment;
    private CursorFactory frameCursorFactory;

    public BasicTests(final FrameType frameType)
    {
      this.frameType = frameType;
    }

    @Parameterized.Parameters(name = "frameType = {0}")
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (FrameType frameType : FrameType.values()) {
        constructors.add(new Object[]{frameType});
      }

      return constructors;
    }

    @Before
    public void setUp()
    {

      queryableCursorFactory = new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex());
      frameSegment = FrameTestUtil.cursorFactoryToFrameSegment(queryableCursorFactory, frameType);
      frameCursorFactory = Objects.requireNonNull(frameSegment.as(CursorFactory.class));
    }

    @After
    public void tearDown()
    {
      if (frameSegment != null) {
        frameSegment.close();
      }
    }

    @Test
    public void test_getRowSignature()
    {
      Assert.assertEquals(queryableCursorFactory.getRowSignature(), frameCursorFactory.getRowSignature());
    }

    @Test
    public void test_getColumnCapabilities_typeOfKnownColumns()
    {
      for (final String columnName : frameCursorFactory.getRowSignature().getColumnNames()) {
        final ColumnCapabilities expectedCapabilities = queryableCursorFactory.getColumnCapabilities(columnName);
        final ColumnCapabilities actualCapabilities = frameCursorFactory.getColumnCapabilities(columnName);

        Assert.assertEquals(
            StringUtils.format("column [%s] type", columnName),
            expectedCapabilities.toColumnType(),
            actualCapabilities.toColumnType()
        );

        if (frameType == FrameType.latestColumnar()) {
          // Columnar frames retain fine-grained hasMultipleValues information
          Assert.assertEquals(
              StringUtils.format("column [%s] hasMultipleValues", columnName),
              expectedCapabilities.hasMultipleValues(),
              actualCapabilities.hasMultipleValues()
          );
        } else {
          // Row-based frames do not retain fine-grained hasMultipleValues information
          Assert.assertEquals(
              StringUtils.format("column [%s] hasMultipleValues", columnName),
              expectedCapabilities.getType() == ValueType.STRING
              ? ColumnCapabilities.Capable.UNKNOWN
              : ColumnCapabilities.Capable.FALSE,
              actualCapabilities.hasMultipleValues()
          );
        }
      }
    }

    @Test
    public void test_getColumnCapabilities_unknownColumn()
    {
      Assert.assertNull(frameCursorFactory.getColumnCapabilities("nonexistent"));
    }
  }

  /**
   * CursorTests: matrix of tests of makeCursor, makeVectorCursor
   */
  @RunWith(Parameterized.class)
  public static class CursorTests extends InitializedNullHandlingTest
  {
    private static final int VECTOR_SIZE = 7;

    private final FrameType frameType;
    private final Interval interval;

    private CursorFactory queryableCursorFactory;
    private FrameSegment frameSegment;
    private CursorFactory frameCursorFactory;
    private final QueryContext queryContext = QueryContext.of(
        ImmutableMap.of(QueryContexts.VECTOR_SIZE_KEY, VECTOR_SIZE)
    );

    public CursorTests(
        FrameType frameType,
        Interval interval
    )
    {
      this.frameType = frameType;
      this.interval = interval;
    }

    @Parameterized.Parameters(
        name = "frameType = {0}, interval = {1}"
    )
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();
      final List<Interval> intervals = List.of(
          TestIndex.getMMappedTestIndex().getDataInterval(),
          Intervals.ETERNITY,
          Intervals.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"),
          Intervals.of("3001/3002")
      );

      for (FrameType frameType : FrameType.values()) {
        for (Interval interval : intervals) {
          constructors.add(
              new Object[]{frameType, interval}
          );
        }
      }

      return constructors;
    }

    @Before
    public void setUp()
    {
      queryableCursorFactory = new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex());
      frameSegment = FrameTestUtil.cursorFactoryToFrameSegment(queryableCursorFactory, frameType);
      frameCursorFactory = Objects.requireNonNull(frameSegment.as(CursorFactory.class));
    }

    @After
    public void tearDown()
    {
      if (frameSegment != null) {
        frameSegment.close();
      }
    }


    @Test
    public void test_fullScan()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_fullScan_preferAscTime()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setPreferredOrdering(Cursors.ascendingTimeOrder())
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_fullScan_preferDescTime()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setPreferredOrdering(Cursors.descendingTimeOrder())
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_selectorFilter_stringColumn()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setFilter(new SelectorDimFilter("quality", "automotive", null).toFilter())
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_selectorFilter_numericColumn()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setFilter(new SelectorDimFilter("qualityLong", "1400", null).toFilter())
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_selectorFilter_expr()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setFilter(new SelectorDimFilter("quality", "automotive", null).toFilter())
                         .setVirtualColumns(
                             VirtualColumns.create(
                                 new ExpressionVirtualColumn(
                                     "expr",
                                     "qualityLong + 1",
                                     ColumnType.LONG,
                                     ExprMacroTable.nil()
                                 )
                             )
                         )
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_selectorFilter_extractionFn()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setFilter(
                             new SelectorDimFilter(
                                 "quality",
                                 "automotive",
                                 new UpperExtractionFn(null)
                             ).toFilter()
                         )
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_selectorFilter_timeExtractionFn()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setFilter(
                             new SelectorDimFilter(
                                 ColumnHolder.TIME_COLUMN_NAME,
                                 "Friday",
                                 new TimeFormatExtractionFn("EEEE", null, null, null, false)
                             ).toFilter()
                         )
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_expr_cannotVectorize()
    {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setVirtualColumns(
                             VirtualColumns.create(
                                 new ExpressionVirtualColumn(
                                     "expr",
                                     "substring(quality, 1, 2)",
                                     ColumnType.STRING,
                                     ExprMacroTable.nil()
                                 )
                             )
                         )
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, false);
    }

    @Test
    public void test_aggregator_canVectorize()
    {
      // Test that when an aggregator can vectorize, canVectorize returns true. This test is not actually testing
      // that the aggregator *works*, because verifyCursorFactory doesn't try to use it.
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setAggregators(List.of(new DoubleSumAggregatorFactory("qualitySum", "qualityLong")))
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, frameType.isColumnar());
    }

    @Test
    public void test_aggregator_cannotVectorize()
    {
      // Test that when an aggregator cannot vectorize, canVectorize returns false. This test is not actually testing
      // that the aggregator *works*, because verifyCursorFactory doesn't try to use it.
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder()
                         .setInterval(interval)
                         .setAggregators(List.of(new DoubleSumAggregatorFactory("qualitySum", "quality")))
                         .setQueryContext(queryContext)
                         .build();

      verifyCursorFactory(cursorSpec, false);
    }

    /**
     * Verify that the cursors (both vector and nonvector) from {@link #frameCursorFactory} and
     * {@link #queryableCursorFactory} match.
     */
    private void verifyCursorFactory(final CursorBuildSpec cursorSpec, final boolean expectedCanVectorize)
    {
      Assert.assertEquals(
          "expected interval (if this assertion fails, the test is likely written incorrectly)",
          this.interval,
          cursorSpec.getInterval()
      );

      // Frame adapters don't know the order of the underlying frames, so they should ignore the "preferred ordering"
      // of the cursor build spec. We test this by passing the frameAdapter a build spec with a preferred ordering,
      // and passing the queryableAdapter the same build spec *without* a preferred ordering, and verifying they match.
      final CursorBuildSpec queryableCursorSpec =
          CursorBuildSpec.builder(cursorSpec).setPreferredOrdering(Collections.emptyList()).build();

      try (final CursorHolder queryableCursorHolder = queryableCursorFactory.makeCursorHolder(queryableCursorSpec);
           final CursorHolder frameCursorHolder = frameCursorFactory.makeCursorHolder(cursorSpec)) {
        // Frames don't know their own order, so they cannot guarantee any particular ordering.
        Assert.assertEquals("ordering", Collections.emptyList(), frameCursorHolder.getOrdering());
        Assert.assertEquals("canVectorize", expectedCanVectorize, frameCursorHolder.canVectorize());
        verifyCursors(queryableCursorHolder, frameCursorHolder, frameCursorFactory.getRowSignature());

        if (expectedCanVectorize) {
          verifyVectorCursors(queryableCursorHolder, frameCursorHolder, frameCursorFactory.getRowSignature());
        }
      }
    }

    /**
     * Verify that the non-vector cursors from two {@link CursorHolder} return equivalent results.
     */
    private static void verifyCursors(
        final CursorHolder queryableCursorHolder,
        final CursorHolder frameCursorHolder,
        final RowSignature signature
    )
    {
      final Sequence<List<Object>> queryableRows =
          FrameTestUtil.readRowsFromCursor(advanceAndReset(queryableCursorHolder.asCursor()), signature);
      final Sequence<List<Object>> frameRows =
          FrameTestUtil.readRowsFromCursor(advanceAndReset(frameCursorHolder.asCursor()), signature);
      FrameTestUtil.assertRowsEqual(queryableRows, frameRows);
    }

    /**
     * Verify that the vector cursors from two {@link CursorHolder} return equivalent results. Only call this
     * if the holders have {@link CursorHolder#canVectorize()}.
     */
    private static void verifyVectorCursors(
        final CursorHolder queryableCursorHolder,
        final CursorHolder frameCursorHolder,
        final RowSignature signature
    )
    {
      final Sequence<List<Object>> queryableRows =
          FrameTestUtil.readRowsFromVectorCursor(advanceAndReset(queryableCursorHolder.asVectorCursor()), signature)
                       .withBaggage(queryableCursorHolder);
      final Sequence<List<Object>> frameRows =
          FrameTestUtil.readRowsFromVectorCursor(advanceAndReset(frameCursorHolder.asVectorCursor()), signature)
                       .withBaggage(frameCursorHolder);
      FrameTestUtil.assertRowsEqual(queryableRows, frameRows);
    }

    /**
     * Advance and reset a Cursor. Helps test that reset() works properly.
     */
    private static Cursor advanceAndReset(final Cursor cursor)
    {
      for (int i = 0; i < 3 && !cursor.isDone(); i++) {
        cursor.advance();
      }

      cursor.reset();
      return cursor;
    }

    /**
     * Advance and reset a VectorCursor. Helps test that reset() works properly.
     */
    private static VectorCursor advanceAndReset(final VectorCursor cursor)
    {
      for (int i = 0; i < 3 && !cursor.isDone(); i++) {
        cursor.advance();
      }

      cursor.reset();
      return cursor;
    }
  }
}
