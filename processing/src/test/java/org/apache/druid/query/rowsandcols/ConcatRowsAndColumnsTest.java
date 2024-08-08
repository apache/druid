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

package org.apache.druid.query.rowsandcols;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

public class ConcatRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public ConcatRowsAndColumnsTest()
  {
    super(ConcatRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, ConcatRowsAndColumns> MAKER = input -> {
    int rowsPerChunk = Math.max(1, input.numRows() / 4);

    ArrayList<RowsAndColumns> theRac = new ArrayList<>();

    int startId = 0;
    while (startId < input.numRows()) {
      theRac.add(new LimitedRowsAndColumns(input, startId, Math.min(input.numRows(), startId + rowsPerChunk)));
      startId += rowsPerChunk;
    }

    return new ConcatRowsAndColumns(theRac);
  };

  @Test
  public void testConstructorWithNullRacBuffer()
  {
    final NullPointerException e = Assert.assertThrows(
        NullPointerException.class,
        () -> new ConcatRowsAndColumns(null)
    );
    Assert.assertEquals("racBuffer cannot be null", e.getMessage());
  }

  @Test
  public void testFindColumn()
  {
    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "column1", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6}),
            "column2", new IntArrayColumn(new int[]{6, 5, 4, 3, 2, 1})
        )
    );
    ConcatRowsAndColumns apply = MAKER.apply(rac);
    Assert.assertEquals(1, apply.findColumn("column1").toAccessor().getInt(0));
    Assert.assertEquals(6, apply.findColumn("column2").toAccessor().getInt(0));
  }

  @Test
  public void testFindColumnWithEmptyRacBuffer()
  {
    ConcatRowsAndColumns concatRowsAndColumns = new ConcatRowsAndColumns(new ArrayList<>());
    Assert.assertNull(concatRowsAndColumns.findColumn("columnName"));
  }

  @Test
  public void testGetColumns()
  {
    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "column1", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "column2", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    );
    ConcatRowsAndColumns apply = MAKER.apply(rac);
    Assert.assertEquals(Arrays.asList("column1", "column2"), new ArrayList<>(apply.getColumnNames()));
  }

  @Test
  public void testGetColumnsWithEmptyRacBuffer()
  {
    ConcatRowsAndColumns concatRowsAndColumns = new ConcatRowsAndColumns(new ArrayList<>());
    Assert.assertTrue(concatRowsAndColumns.getColumnNames().isEmpty());
  }
}
