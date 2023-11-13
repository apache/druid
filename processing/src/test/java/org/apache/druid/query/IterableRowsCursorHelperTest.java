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
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class IterableRowsCursorHelperTest
{

  List<Object[]> rows = ImmutableList.of(
      new Object[]{1, "a"},
      new Object[]{3, "b"},
      new Object[]{2, "b"}
  );

  RowSignature rowSignature = RowSignature.builder()
                                          .add("dim1", ColumnType.LONG)
                                          .add("dim2", ColumnType.STRING)
                                          .build();

  @Test
  public void getCursorFromIterable()
  {
    Cursor cursor = IterableRowsCursorHelper.getCursorFromIterable(rows, rowSignature).lhs;
    testCursorMatchesRowSequence(cursor, rowSignature, rows);
  }

  @Test
  public void getCursorFromSequence()
  {

    Cursor cursor = IterableRowsCursorHelper.getCursorFromSequence(Sequences.simple(rows), rowSignature).lhs;
    testCursorMatchesRowSequence(cursor, rowSignature, rows);
  }

  @Test
  public void getCursorFromYielder()
  {
    Cursor cursor = IterableRowsCursorHelper.getCursorFromYielder(Yielders.each(Sequences.simple(rows)), rowSignature).lhs;
    testCursorMatchesRowSequence(cursor, rowSignature, rows);
  }

  private void testCursorMatchesRowSequence(
      Cursor cursor,
      RowSignature expectedRowSignature,
      List<Object[]> expectedRows
  )
  {
    List<Object[]> actualRows = new ArrayList<>();
    while (!cursor.isDone()) {
      Object[] row = new Object[expectedRowSignature.size()];
      for (int i = 0; i < expectedRowSignature.size(); ++i) {
        ColumnValueSelector columnValueSelector = cursor.getColumnSelectorFactory()
                                                        .makeColumnValueSelector(expectedRowSignature.getColumnName(i));
        row[i] = columnValueSelector.getObject();
      }
      actualRows.add(row);
      cursor.advance();
    }
    QueryToolChestTestHelper.assertArrayResultsEquals(expectedRows, Sequences.simple(actualRows));
  }
}
