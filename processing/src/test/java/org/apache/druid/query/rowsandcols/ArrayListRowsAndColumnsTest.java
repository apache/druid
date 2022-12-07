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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.function.Function;

public class ArrayListRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public ArrayListRowsAndColumnsTest()
  {
    super(ArrayListRowsAndColumns.class);
  }

  @Nonnull
  public static Function<MapOfColumnsRowsAndColumns, ArrayListRowsAndColumns<Object[]>> MAKER = input -> {
    ArrayList<Object[]> rows = new ArrayList<>(input.numRows());

    ArrayList<String> cols = new ArrayList<>(input.getColumnNames());
    final RowSignature.Builder sigBob = RowSignature.builder();

    for (int i = 0; i < input.numRows(); ++i) {
      rows.add(new Object[cols.size()]);
    }

    for (int colIndex = 0; colIndex < cols.size(); ++colIndex) {
      String col = cols.get(colIndex);
      final ColumnAccessor column = input.findColumn(col).toAccessor();
      sigBob.add(col, column.getType());

      for (int i = 0; i < column.numRows(); ++i) {
        rows.get(i)[colIndex] = column.getObject(i);
      }
    }

    return new ArrayListRowsAndColumns<>(
        rows,
        columnName -> {
          final int i = cols.indexOf(columnName);
          if (i < 0) {
            throw new ISE("Couldn't find column[%s]!? i[%s]", columnName, i);
          }
          return objects -> objects[i];
        },
        sigBob.build()
    );
  };
}
