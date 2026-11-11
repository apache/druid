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

public class WindowLastProcessorTest
{
  @Test
  public void testLastProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9.84}));
    map.put("objectCol", new ObjectArrayColumn(
        new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING
    ));
    map.put("nullLastCol", new ObjectArrayColumn(
        new String[]{null, "b", "c", "d", "e", "f", "g", "h", "i", null},
        ColumnType.STRING
    ));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    ComposingProcessor processor = new ComposingProcessor(
        new WindowLastProcessor("intCol", "LastIntCol", null),
        new WindowLastProcessor("doubleCol", "LastDoubleCol", null),
        new WindowLastProcessor("objectCol", "LastObjectCol", null),
        new WindowLastProcessor("nullLastCol", "NullLastCol", null)
    );
    Assertions.assertEquals(
        ImmutableList.of("LastIntCol", "LastDoubleCol", "LastObjectCol", "NullLastCol"),
        processor.getOutputColumnNames()
    );


    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9.84})
        .expectColumn("LastIntCol", new int[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("LastDoubleCol", new double[]{9.84, 9.84, 9.84, 9.84, 9.84, 9.84, 9.84, 9.84, 9.84, 9.84});

    expectations.columnHelper("LastObjectCol", 10, ColumnType.STRING)
                .setExpectation(new String[]{"j", "j", "j", "j", "j", "j", "j", "j", "j", "j"});

    expectations.columnHelper("NullLastCol", 10, ColumnType.STRING)
                .setNulls(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);
  }

  @Test
  public void testLastWithRowsFramePrecedingAndFollowing()
  {
    // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(-1, 1));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: frame [0,1] -> "b"; Row 1: frame [0,2] -> "c"; Row 2: frame [1,3] -> "d";
    // Row 3: frame [2,4] -> "e"; Row 4: frame [3,4] -> "e"
    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "b", "c", "d", "e", "e")
        .validate(results);
  }

  @Test
  public void testLastWithRowsFrameFullyPreceding()
  {
    // ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING — frame excludes current row
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(-2, -1));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: empty -> null; Row 1: [0,0] -> "a"; Row 2: [0,1] -> "b";
    // Row 3: [1,2] -> "c"; Row 4: [2,3] -> "d"
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{null, "a", "b", "c", "d"}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testLastWithRowsFrameFullyFollowing()
  {
    // ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING — frame excludes current row
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(1, 2));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: [1,2] -> "c"; Row 1: [2,3] -> "d"; Row 2: [3,4] -> "e";
    // Row 3: [4,4] -> "e"; Row 4: empty -> null
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{"c", "d", "e", "e", null}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testLastWithUnboundedRowsFrame()
  {
    // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"a", "b", "c", "d", "e"});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(null, null));

    final RowsAndColumns results = processor.process(rac);

    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "e", "e", "e", "e", "e")
        .validate(results);
  }

  @Test
  public void testLastWithGroupsFrame()
  {
    // GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING
    // Data grouped by "grp": groups are [a,a], [b,b], [c]
    final Map<String, Column> map = new LinkedHashMap<>();
    map.put("grp", new ObjectArrayColumn(new String[]{"a", "a", "b", "b", "c"}, ColumnType.STRING));
    map.put("val", new ObjectArrayColumn(new String[]{"v1", "v2", "v3", "v4", "v5"}, ColumnType.STRING));
    final MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    final WindowLastProcessor processor = new WindowLastProcessor(
        "val",
        "out",
        WindowFrame.groups(0, 1, Collections.singletonList("grp"))
    );

    final RowsAndColumns results = processor.process(rac);

    // Group 0 (rows 0,1): groups [0,1] -> last of group 1 = "v4"
    // Group 1 (rows 2,3): groups [1,2] -> last of group 2 = "v5"
    // Group 2 (row 4):    groups [2,2] -> last of group 2 = "v5"
    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "v4", "v4", "v5", "v5", "v5")
        .validate(results);
  }

  @Test
  public void testLastWithSingleRowAndBoundedFrame()
  {
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{"only"});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(-2, 2));

    final RowsAndColumns results = processor.process(rac);

    new RowsAndColumnsHelper()
        .expectColumn("out", ColumnType.STRING, "only")
        .validate(results);
  }

  @Test
  public void testLastWithNullsInRowsFrame()
  {
    // SQL standard: LAST_VALUE returns the value at the last row of the frame,
    // including NULL (RESPECT NULLS is the default).
    // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{null, "b", null, "d", null});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(-1, 1));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: frame [0,1], last = "b"  (row 1)
    // Row 1: frame [0,2], last = null (row 2)
    // Row 2: frame [1,3], last = "d"  (row 3)
    // Row 3: frame [2,4], last = null (row 4)
    // Row 4: frame [3,4], last = null (row 4)
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{"b", null, "d", null, null}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testLastWithNullsAndEmptyRowsFrame()
  {
    // Tests both null-valued last rows and empty frames (which also yield null).
    // ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING
    final MapOfColumnsRowsAndColumns rac = makeStringRac(new String[]{null, "b", null, "d", null});
    final WindowLastProcessor processor =
        new WindowLastProcessor("col", "out", WindowFrame.rows(2, 3));

    final RowsAndColumns results = processor.process(rac);

    // Row 0: frame [2,3], last = "d"  (row 3)
    // Row 1: frame [3,4], last = null (row 4)
    // Row 2: frame [4,4], last = null (row 4)
    // Row 3: empty -> null
    // Row 4: empty -> null
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{"d", null, null, null, null}, ColumnType.STRING)
        .validate(results);
  }

  @Test
  public void testLastWithNullsInGroupsFrame()
  {
    // GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING
    // Groups: [a,a], [b,b], [c]. Last row of some groups has a null value.
    final Map<String, Column> map = new LinkedHashMap<>();
    map.put("grp", new ObjectArrayColumn(new String[]{"a", "a", "b", "b", "c"}, ColumnType.STRING));
    map.put("val", new ObjectArrayColumn(new String[]{"v1", null, "v3", null, "v5"}, ColumnType.STRING));
    final MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    final WindowLastProcessor processor = new WindowLastProcessor(
        "val",
        "out",
        WindowFrame.groups(0, 1, Collections.singletonList("grp"))
    );

    final RowsAndColumns results = processor.process(rac);

    // Group 0 (rows 0,1): groups [0,1] -> last of group 1 = null (row 3)
    // Group 1 (rows 2,3): groups [1,2] -> last of group 2 = "v5" (row 4)
    // Group 2 (row 4):    groups [2,2] -> last of group 2 = "v5" (row 4)
    new RowsAndColumnsHelper()
        .expectColumn("out", new Object[]{null, null, "v5", "v5", "v5"}, ColumnType.STRING)
        .validate(results);
  }

  private static MapOfColumnsRowsAndColumns makeStringRac(String[] values)
  {
    final Map<String, Column> map = new LinkedHashMap<>();
    map.put("col", new ObjectArrayColumn(values, ColumnType.STRING));
    return MapOfColumnsRowsAndColumns.fromMap(map);
  }
}
