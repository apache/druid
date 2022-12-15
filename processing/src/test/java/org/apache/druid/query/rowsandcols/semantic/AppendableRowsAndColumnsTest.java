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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Test;

import java.util.function.Function;

public class AppendableRowsAndColumnsTest extends SemanticTestBase
{
  public AppendableRowsAndColumnsTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testAppendableRowsAndColumns()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "colA", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            "colB", new IntArrayColumn(new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        )
    ));

    AppendableRowsAndColumns appender = RowsAndColumns.expectAppendable(rac);

    appender.addColumn("newCol", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

    new RowsAndColumnsHelper()
        .expectColumn("colA", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        .expectColumn("colB", new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        .expectColumn("newCol", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        .allColumnsRegistered()
        .validate(appender);
  }

}
