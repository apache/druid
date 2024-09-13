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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.extraction.UpperExtractionFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
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
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

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
      frameCursorFactory = frameSegment.asCursorFactory();
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

        if (frameType == FrameType.COLUMNAR) {
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
    @Nullable
    private final Filter filter;
    private final Interval interval;
    private final VirtualColumns virtualColumns;
    private final boolean descending;

    private CursorFactory queryableCursorFactory;
    private FrameSegment frameSegment;
    private CursorFactory frameCursorFactory;
    private final QueryContext queryContext = QueryContext.of(
        ImmutableMap.of(QueryContexts.VECTOR_SIZE_KEY, VECTOR_SIZE)
    );

    private CursorBuildSpec buildSpec;

    public CursorTests(
        FrameType frameType,
        @Nullable DimFilter filter,
        Interval interval,
        VirtualColumns virtualColumns,
        boolean descending
    )
    {
      this.frameType = frameType;
      this.filter = Filters.toFilter(filter);
      this.interval = interval;
      this.virtualColumns = virtualColumns;
      this.descending = descending;
      this.buildSpec =
          CursorBuildSpec.builder()
                         .setFilter(this.filter)
                         .setInterval(this.interval)
                         .setVirtualColumns(this.virtualColumns)
                         .setPreferredOrdering(descending ? Cursors.descendingTimeOrder() : Collections.emptyList())
                         .setQueryContext(queryContext)
                         .build();
    }

    @Parameterized.Parameters(name = "frameType = {0}, "
                                     + "filter = {1}, "
                                     + "interval = {2}, "
                                     + "virtualColumns = {3}, "
                                     + "descending = {4}")
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();
      final List<Interval> intervals = Arrays.asList(
          TestIndex.getMMappedTestIndex().getDataInterval(),
          Intervals.ETERNITY,
          Intervals.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"),
          Intervals.of("3001/3002")
      );

      final List<Pair<DimFilter, VirtualColumns>> filtersAndVirtualColumns = new ArrayList<>();
      filtersAndVirtualColumns.add(Pair.of(null, VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter("quality", "automotive", null), VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(
          new SelectorDimFilter("expr", "1401", null),
          VirtualColumns.create(
              ImmutableList.of(
                  new ExpressionVirtualColumn(
                      "expr",
                      "qualityLong + 1",
                      ColumnType.LONG,
                      ExprMacroTable.nil()
                  )
              )
          )
      ));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter("qualityLong", "1400", null), VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(
          new SelectorDimFilter("quality", "automotive", new UpperExtractionFn(null)),
          VirtualColumns.EMPTY
      ));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter(
          ColumnHolder.TIME_COLUMN_NAME,
          "Friday",
          new TimeFormatExtractionFn("EEEE", null, null, null, false)
      ), VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter(
          ColumnHolder.TIME_COLUMN_NAME,
          "Friday",
          new TimeFormatExtractionFn("EEEE", null, null, null, false)
      ), VirtualColumns.EMPTY));

      for (FrameType frameType : FrameType.values()) {
        for (Pair<DimFilter, VirtualColumns> filterVirtualColumnsPair : filtersAndVirtualColumns) {
          for (Interval interval : intervals) {
            for (boolean descending : Arrays.asList(false, true)) {
              constructors.add(
                  new Object[]{
                      frameType,
                      filterVirtualColumnsPair.lhs,
                      interval,
                      filterVirtualColumnsPair.rhs,
                      descending
                  }
              );
            }
          }
        }
      }

      return constructors;
    }

    @Before
    public void setUp()
    {
      queryableCursorFactory = new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex());
      frameSegment = FrameTestUtil.cursorFactoryToFrameSegment(queryableCursorFactory, frameType);
      frameCursorFactory = frameSegment.asCursorFactory();
    }

    @After
    public void tearDown()
    {
      if (frameSegment != null) {
        frameSegment.close();
      }
    }

    @Test
    public void test_makeCursor()
    {
      final RowSignature signature = frameCursorFactory.getRowSignature();

      // Frame adapters don't know the order of the underlying frames, so they should ignore the "preferred ordering"
      // of the cursor build spec. We test this by passing the frameAdapter a build spec with a preferred ordering,
      // and passing the queryableAdapter the same build spec *without* a preferred ordering, and verifying they match.
      final CursorBuildSpec queryableBuildSpec =
          CursorBuildSpec.builder(buildSpec).setPreferredOrdering(Collections.emptyList()).build();

      try (final CursorHolder queryableCursorHolder = queryableCursorFactory.makeCursorHolder(queryableBuildSpec);
           final CursorHolder frameCursorHolder = frameCursorFactory.makeCursorHolder(buildSpec)) {
        final Sequence<List<Object>> queryableRows =
            FrameTestUtil.readRowsFromCursor(advanceAndReset(queryableCursorHolder.asCursor()), signature);
        final Sequence<List<Object>> frameRows =
            FrameTestUtil.readRowsFromCursor(advanceAndReset(frameCursorHolder.asCursor()), signature);
        FrameTestUtil.assertRowsEqual(queryableRows, frameRows);
      }
    }

    @Test
    public void test_makeVectorCursor()
    {
      // Conditions for frames to be vectorizable.
      Assume.assumeThat(frameType, CoreMatchers.equalTo(FrameType.COLUMNAR));
      Assume.assumeFalse(descending);
      assertVectorCursorsMatch(cursorFactory -> cursorFactory.makeCursorHolder(buildSpec));
    }

    private void assertVectorCursorsMatch(final Function<CursorFactory, CursorHolder> call)
    {
      final CursorHolder cursorHolder = call.apply(queryableCursorFactory);
      final CursorHolder frameCursorHolder = call.apply(frameCursorFactory);

      Assert.assertTrue("queryable cursor is vectorizable", cursorHolder.canVectorize());
      Assert.assertTrue("frame cursor is vectorizable", frameCursorHolder.canVectorize());

      final RowSignature signature = frameCursorFactory.getRowSignature();
      final Sequence<List<Object>> queryableRows =
          FrameTestUtil.readRowsFromVectorCursor(advanceAndReset(cursorHolder.asVectorCursor()), signature)
                       .withBaggage(cursorHolder);
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
