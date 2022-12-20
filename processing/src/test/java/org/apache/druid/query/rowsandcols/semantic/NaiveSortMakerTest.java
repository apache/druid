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

import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.function.Function;

public class NaiveSortMakerTest extends SemanticTestBase
{
  public NaiveSortMakerTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testSortMultipleChunks()
  {
    final RowsAndColumns first = make(MapOfColumnsRowsAndColumns.of(
        "ints", new IntArrayColumn(new int[]{1, 7, 13, 0, 19}),
        "strs", new ObjectArrayColumn(new Object[]{"b", "h", "n", "a", "t"}, ColumnType.STRING)
    ));

    NaiveSortMaker maker = first.as(NaiveSortMaker.class);
    if (maker == null) {
      maker = new DefaultNaiveSortMaker(first);
    }

    final NaiveSortMaker.NaiveSorter intSorter = maker.make(
        new OrderByColumnSpec("ints", OrderByColumnSpec.Direction.ASCENDING)
    );
    final NaiveSortMaker.NaiveSorter stringSorter = maker.make(
        new OrderByColumnSpec("ints", OrderByColumnSpec.Direction.ASCENDING)
    );

    RowsAndColumns intermediate = make(MapOfColumnsRowsAndColumns.of(
        "ints", new IntArrayColumn(new int[]{2, 3, 16, 4, 5}),
        "strs", new ObjectArrayColumn(new Object[]{"c", "d", "q", "e", "f"}, ColumnType.STRING)
    ));
    intSorter.moreData(intermediate);
    stringSorter.moreData(intermediate);

    intermediate = make(MapOfColumnsRowsAndColumns.of(
        "ints", new IntArrayColumn(new int[]{10, 17, 12, 8, 14, 15}),
        "strs", new ObjectArrayColumn(new Object[]{"k", "r", "m", "i", "o", "p"}, ColumnType.STRING)
    ));
    intSorter.moreData(intermediate);
    stringSorter.moreData(intermediate);

    intermediate = make(MapOfColumnsRowsAndColumns.of(
        "ints", new IntArrayColumn(new int[]{6, 18, 11, 14, 9}),
        "strs", new ObjectArrayColumn(new Object[]{"g", "s", "l", "o", "j"}, ColumnType.STRING)
    ));
    intSorter.moreData(intermediate);
    stringSorter.moreData(intermediate);

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper()
        .expectColumn("ints", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 14, 15, 16, 17, 18, 19})
        .expectColumn(
            "strs",
            new Object[]{
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "o", "p", "q", "r", "s", "t"
            },
            ColumnType.STRING
        )
        .allColumnsRegistered();

    final RowsAndColumns intSorted = intSorter.complete();
    helper.validate(intSorted);

    final RowsAndColumns strSorted = stringSorter.complete();
    helper.validate(strSorted);
  }

  @Test
  public void testSortOneChunk()
  {
    final RowsAndColumns first = make(MapOfColumnsRowsAndColumns.of(
        "ints", new IntArrayColumn(new int[]{1, 7, 13, 0, 19}),
        "strs", new ObjectArrayColumn(new Object[]{"b", "h", "n", "a", "t"}, ColumnType.STRING)
    ));

    NaiveSortMaker maker = first.as(NaiveSortMaker.class);
    if (maker == null) {
      maker = new DefaultNaiveSortMaker(first);
    }

    final NaiveSortMaker.NaiveSorter sorter = maker.make(
        new OrderByColumnSpec("ints", OrderByColumnSpec.Direction.ASCENDING)
    );
    final NaiveSortMaker.NaiveSorter stringSorter = maker.make(
        new OrderByColumnSpec("ints", OrderByColumnSpec.Direction.ASCENDING)
    );


    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper()
        .expectColumn("ints", new int[]{0, 1, 7, 13, 19})
        .expectColumn("strs", new Object[]{"a", "b", "h", "n", "t"}, ColumnType.STRING)
        .allColumnsRegistered();

    final RowsAndColumns sorted = sorter.complete();
    helper.validate(sorted);

    final RowsAndColumns stringSorted = stringSorter.complete();
    helper.validate(stringSorted);
  }
}