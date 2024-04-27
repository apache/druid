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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.operator.window.WindowFrame.PeerType;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class FramedOnHeapAggregatableTest extends SemanticTestBase
{

  public FramedOnHeapAggregatableTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testWindowedAggregationWindowSmallerThanRowsNoOffsets()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 0, false, 0, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("maxFromInt", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationWindowSmallerThanRows()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 1, false, 2, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{3, 6, 10, 14, 18, 22, 26, 30, 24, 17})
        .expectColumn("maxFromInt", new double[]{2, 3, 4, 5, 6, 7, 8, 9, 9, 9})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationWindowSmallerThanRowsOnlyUpper()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 0, false, 2, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{3, 6, 9, 12, 15, 18, 21, 24, 17, 9})
        .expectColumn("maxFromInt", new double[]{2, 3, 4, 5, 6, 7, 8, 9, 9, 9})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationWindowSmallerThanRowsOnlyLower()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 2, false, 0, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{0, 1, 3, 6, 9, 12, 15, 18, 21, 24})
        .expectColumn("maxFromInt", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationWindowLargerThanRows()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 5, false, 7, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{28, 36, 45, 45, 45, 45, 45, 44, 42, 39})
        .expectColumn("maxFromInt", new double[]{7, 8, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("longMin", new long[]{0, 0, 0, 0, 0, 0, 1, 2, 3, 4})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationLowerLargerThanRows()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 5, false, 1, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2})
        .expectColumn("sumFromLong", new long[]{1, 3, 3})
        .expectColumn("maxFromInt", new double[]{1, 2, 2})
        .expectColumn("longMin", new long[]{0, 0, 0})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationLowerLargerThanRowsNoUpper()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 5, false, 0, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2})
        .expectColumn("sumFromLong", new long[]{0, 1, 3})
        .expectColumn("maxFromInt", new double[]{0, 1, 2})
        .expectColumn("longMin", new long[]{0, 0, 0})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationUpperLargerThanRows()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 1, false, 7, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2})
        .expectColumn("sumFromLong", new long[]{3, 3, 3})
        .expectColumn("maxFromInt", new double[]{2, 2, 2})
        .expectColumn("longMin", new long[]{0, 0, 1})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationUpperLargerThanRowsNoLower()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 0, false, 7, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2})
        .expectColumn("sumFromLong", new long[]{3, 3, 2})
        .expectColumn("maxFromInt", new double[]{2, 2, 2})
        .expectColumn("longMin", new long[]{0, 1, 2})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationWindowLargerThanRowsOnlyUpper()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 0, false, 7, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{28, 36, 44, 42, 39, 35, 30, 24, 17, 9})
        .expectColumn("maxFromInt", new double[]{7, 8, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("longMin", new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testWindowedAggregationWindowLargerThanRowsOnlyLower()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 5, false, 0, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new LongMinAggregatorFactory("longMin", "intCol"),
            }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{0, 1, 3, 6, 10, 15, 21, 27, 33, 39})
        .expectColumn("maxFromInt", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("longMin", new long[]{0, 0, 0, 0, 0, 0, 1, 2, 3, 4})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testUnboundedWindowedAggregation()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
                new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
                ColumnType.STRING
            )
    );

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new LongSumAggregatorFactory("sumFromDouble", "doubleCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromDouble", "doubleCol")
        }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("objectCol", new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, ColumnType.STRING)
        .expectColumn("sumFromLong", new long[]{45, 45, 45, 45, 45, 45, 45, 45, 45, 45})
        .expectColumn("sumFromDouble", new long[]{45, 45, 45, 45, 45, 45, 45, 45, 45, 45})
        .expectColumn("maxFromInt", new double[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("maxFromDouble", new double[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testCumulativeAggregation()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
                new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
                ColumnType.STRING
            )
    );

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, false, 0, null),
        new AggregatorFactory[]{
            new LongMaxAggregatorFactory("cummMax", "intCol"),
            new DoubleSumAggregatorFactory("cummSum", "doubleCol")
        }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("objectCol", new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, ColumnType.STRING)
        .expectColumn("cummMax", new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("cummSum", new double[]{0, 1, 3, 6, 10, 15, 21, 28, 36, 45})
        .allColumnsRegistered()
        .validate(results);
  }

  @Test
  public void testReverseCumulativeAggregation()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
                new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
                ColumnType.STRING
            )
    );

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        new WindowFrame(WindowFrame.PeerType.ROWS, false, 0, true, 0, null),
        new AggregatorFactory[]{
            new LongMaxAggregatorFactory("cummMax", "intCol"),
            new DoubleSumAggregatorFactory("cummSum", "doubleCol")
        }
    );

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("objectCol", new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, ColumnType.STRING)
        .expectColumn("cummMax", new long[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("cummSum", new double[]{45, 45, 44, 42, 39, 35, 30, 24, 17, 9})
        .allColumnsRegistered()
        .validate(results);
  }



  @Test
  public void testRangeOrderBy()
  {
    WindowFrame frame = WindowFrame.forOrderBy(ColumnWithDirection.ascending("c1"));
    int[] c1Vals = new int[] {0, 0, 0, 1, 1, 1, 2, 2, 2, 2};
    int[] c2Vals = new int[] {1, 1, 2, 1, 1, 2, 1, 1, 1, 2};
    int[] resVals = new int[] {4, 4, 4, 8, 8, 8, 13, 13, 13, 13};

    simpleWindowingTest(frame, c1Vals, c2Vals, resVals);
  }

  @Test
  public void testRangeB1()
  {
    WindowFrame frame = new WindowFrame(
        PeerType.RANGE,
        false,
        1,
        false,
        0,
        Collections.singletonList(ColumnWithDirection.ascending("c1"))
    );

    int[] c1Vals = new int[] {0, 1, 2, 2, 3, 4, 5};
    int[] c2Vals = new int[] {0, 1, 1, 1, 3, 4, 5};
    int[] resVals = new int[] {0, 1, 3, 3, 5, 7, 9};

    simpleWindowingTest(frame, c1Vals, c2Vals, resVals);
  }

  @Test
  public void testRangeA1()
  {
    WindowFrame frame = new WindowFrame(
        PeerType.RANGE,
        false,
        0,
        false,
        1,
        Collections.singletonList(ColumnWithDirection.ascending("c1"))
    );

    int[] c1Vals = new int[] {0, 1, 2, 2, 3, 4, 5};
    int[] c2Vals = new int[] {0, 1, 1, 1, 3, 4, 5};
    int[] resVals = new int[] {1, 3, 5, 5, 7, 9, 5};

    simpleWindowingTest(frame, c1Vals, c2Vals, resVals);
  }

  @Test
  public void testRangeB1A1()
  {
    WindowFrame frame = new WindowFrame(
        PeerType.RANGE,
        false,
        1,
        false,
        1,
        Collections.singletonList(ColumnWithDirection.ascending("c1"))
    );

    int[] c1Vals = new int[] {0, 1, 2, 3, 4, 5};
    int[] c2Vals = new int[] {0, 1, 2, 3, 4, 5};
    int[] resVals = new int[] {1, 3, 6, 9, 12, 9};

    simpleWindowingTest(frame, c1Vals, c2Vals, resVals);
  }


  @Test
  public void testRangeB1A1_2()
  {
    WindowFrame frame = new WindowFrame(
        PeerType.RANGE,
        false,
        1,
        false,
        1,
        Collections.singletonList(ColumnWithDirection.ascending("c1"))
    );

    int[] c1Vals = new int[] {0, 0, 1, 2, 3, 3, 4, 4, 5};
    int[] c2Vals = new int[] {0, 0, 1, 2, 2, 1, 2, 2, 5};
    int[] resVals = new int[] {1, 1, 3, 6, 9, 9, 12, 12, 9};

    simpleWindowingTest(frame, c1Vals, c2Vals, resVals);
  }

  @Test
  public void testRangeB1A2()
  {
    WindowFrame frame = new WindowFrame(
        PeerType.RANGE,
        false,
        1,
        false,
        2,
        Collections.singletonList(ColumnWithDirection.ascending("c1"))
    );

    int[] c1Vals = new int[] {0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3};
    int[] c2Vals = new int[] {1, 1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1};
    int[] resVals = new int[] {13, 13, 13, 16, 16, 16, 12, 12, 12, 12, 8, 8, 8};

    simpleWindowingTest(frame, c1Vals, c2Vals, resVals);
  }

  private void simpleWindowingTest(WindowFrame frame, int[] c1Vals, int[] c2Vals, int[] resVals)
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("c1", new IntArrayColumn(c1Vals));
    map.put("c2", new IntArrayColumn(c2Vals));

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(map));

    FramedOnHeapAggregatable agger = FramedOnHeapAggregatable.fromRAC(rac);

    final RowsAndColumns results = agger.aggregateAll(
        frame,
        new AggregatorFactory[] {
            new LongSumAggregatorFactory("res", "c2")
        }
    );

    new RowsAndColumnsHelper()
    .expectColumn("c1", c1Vals)
    .expectColumn("c2", c2Vals)
    .expectColumn("res", resVals)
        .allColumnsRegistered()
        .validate(results);
  }

}
