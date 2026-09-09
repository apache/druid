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

package org.apache.druid.query.operator.window.value;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.operator.window.ComposingProcessor;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowFirstProcessorTest
{
  @Test
  public void testFirstProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
        new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING
    ));
    map.put("nullFirstCol", new ObjectArrayColumn(
        new String[]{null, "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING
    ));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    ComposingProcessor processor = new ComposingProcessor(
        new WindowFirstProcessor("intCol", "FirstIntCol", null),
        new WindowFirstProcessor("doubleCol", "FirstDoubleCol", null),
        new WindowFirstProcessor("objectCol", "FirstObjectCol", null),
        new WindowFirstProcessor("nullFirstCol", "NullFirstCol", null)
    );

    Assertions.assertEquals(
        ImmutableList.of("FirstIntCol", "FirstDoubleCol", "FirstObjectCol", "NullFirstCol"),
        processor.getOutputColumnNames()
    );

    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("FirstIntCol", new int[]{88, 88, 88, 88, 88, 88, 88, 88, 88, 88})
        .expectColumn(
            "FirstDoubleCol",
            new double[]{0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728}
        );

    expectations.columnHelper("FirstObjectCol", 10, ColumnType.STRING)
                .setExpectation(new String[]{"a", "a", "a", "a", "a", "a", "a", "a", "a", "a"});

    expectations.columnHelper("NullFirstCol", 10, ColumnType.STRING)
                .setNulls(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);
  }

  @Test
  public void testFirstWithRowsFramePrecedingAndFollowing()
  {
    // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(-1, 1));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: frame [0,1] -> "a"; Row 1: frame [0,2] -> "a"; Row 2: frame [1,3] -> "b";
    // Row 3: frame [2,4] -> "c"; Row 4: frame [3,4] -> "d"
    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "a", "a", "b", "c", "d")
        .validate(results);
  }

  @Test
  public void testFirstWithRowsFrameFullyPreceding()
  {
    // ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING — frame excludes current row
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(-2, -1));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: empty -> null; Row 1: [0,0] -> "a"; Row 2: [0,1] -> "a";
    // Row 3: [1,2] -> "b"; Row 4: [2,3] -> "c"
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{null, "a", "a", "b", "c"}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testFirstWithRowsFrameFullyFollowing()
  {
    // ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING — frame excludes current row
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(1, 2));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: [1,2] -> "b"; Row 1: [2,3] -> "c"; Row 2: [3,4] -> "d";
    // Row 3: [4,4] -> "e"; Row 4: empty -> null
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{"b", "c", "d", "e", null}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testFirstWithUnboundedRowsFrame()
  {
    // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(null, null));

    final RowsAndColumns results = processor.process(rac);

    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "a", "a", "a", "a", "a")
        .validate(results);
  }

  @Test
  public void testFirstWithGroupsFrame()
  {
    // GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING
    // Data grouped by "grp": groups are [a,a], [b,b], [c]
    final Map<String, Column> map = new LinkedHashMap<>();
    map.put("grp", new ObjectArrayColumn(new String[]{"a", "a", "b", "b", "c"}, ColumnType.STRING));
    map.put("val", new ObjectArrayColumn(new String[]{"v1", "v2", "v3", "v4", "v5"}, ColumnType.STRING));
    final MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    final WindowFirstProcessor processor = new WindowFirstProcessor(
        "val",
        "out",
        WindowFrame.groups(0, 1, Collections.singletonList("grp"))
    );

    final RowsAndColumns results = processor.process(rac);

    // Group 0 (rows 0,1): groups [0,1] -> first of group 0 = "v1"
    // Group 1 (rows 2,3): groups [1,2] -> first of group 1 = "v3"
    // Group 2 (row 4):    groups [2,2] -> first of group 2 = "v5"
    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "v1", "v1", "v3", "v3", "v5")
        .validate(results);
  }

  @Test
  public void testFirstWithSingleRowAndBoundedFrame()
  {
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"only"});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(-2, 2));

    final RowsAndColumns results = processor.process(rac);

    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "only")
        .validate(results);
  }

  @Test
  public void testFirstWithNullsInRowsFrame()
  {
    // SQL standard: FIRST_VALUE returns the value at the first row of the frame,
    // including NULL (RESPECT NULLS is the default).
    // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{null, "b", null, "d", null});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(-1, 1));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: frame [0,1], first = null (row 0)
    // Row 1: frame [0,2], first = null (row 0)
    // Row 2: frame [1,3], first = "b"  (row 1)
    // Row 3: frame [2,4], first = null (row 2)
    // Row 4: frame [3,4], first = "d"  (row 3)
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{null, null, "b", null, "d"}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testFirstWithNullsAndEmptyRowsFrame()
  {
    // Tests both null-valued first rows and empty frames (which also yield null).
    // ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{null, "b", null, "d", null});
    final WindowFirstProcessor processor =
        new WindowFirstProcessor("col", "out", WindowFrame.rows(2, 3));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: frame [2,3], first = null (row 2)
    // Row 1: frame [3,4], first = "d"  (row 3)
    // Row 2: frame [4,4], first = null (row 4)
    // Row 3: empty -> null
    // Row 4: empty -> null
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{null, "d", null, null, null}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testFirstWithNullsInGroupsFrame()
  {
    // GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING
    // Groups: [a,a], [b,b], [c]. First row of each group's frame has a null value.
    final Map<String, Column> map = new LinkedHashMap<>();
    map.put("grp", new ObjectArrayColumn(new String[]{"a", "a", "b", "b", "c"}, ColumnType.STRING));
    map.put("val", new ObjectArrayColumn(new String[]{null, "v2", "v3", "v4", "v5"}, ColumnType.STRING));
    final MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    final WindowFirstProcessor processor = new WindowFirstProcessor(
        "val",
        "out",
        WindowFrame.groups(0, 1, Collections.singletonList("grp"))
    );

    final RowsAndColumns results = processor.process(rac);

    // Group 0 (rows 0,1): groups [0,1] -> first of group 0 = null (row 0)
    // Group 1 (rows 2,3): groups [1,2] -> first of group 1 = "v3" (row 2)
    // Group 2 (row 4):    groups [2,2] -> first of group 2 = "v5" (row 4)
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{null, null, "v3", "v3", "v5"}, ColumnType.STRING)
        .validate(results);
  }

  private static MapOfColumnsRowsAndColumns makeStringRac(String[] values)
  {
    final Map<String, Column> map = new LinkedHashMap<>();
    map.put("col", new ObjectArrayColumn(values, ColumnType.STRING));
    return MapOfColumnsRowsAndColumns.fromMap(map);
  }
}
