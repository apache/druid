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

package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.operator.window.ComposingProcessor;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class WindowPercentileProcessorTest
{
  @Test
  public void testPercentileProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
        new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING
    ));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    Processor processor = new ComposingProcessor(
        new WindowPercentileProcessor("1", 1),
        new WindowPercentileProcessor("2", 2),
        new WindowPercentileProcessor("3", 3),
        new WindowPercentileProcessor("4", 4),
        new WindowPercentileProcessor("5", 5),
        new WindowPercentileProcessor("6", 6),
        new WindowPercentileProcessor("7", 7),
        new WindowPercentileProcessor("8", 8),
        new WindowPercentileProcessor("9", 9),
        new WindowPercentileProcessor("10", 10),
        new WindowPercentileProcessor("10292", 10292)
    );

    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("1", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
        .expectColumn("2", new int[]{1, 1, 1, 1, 1, 2, 2, 2, 2, 2})
        .expectColumn("3", new int[]{1, 1, 1, 1, 2, 2, 2, 3, 3, 3})
        .expectColumn("4", new int[]{1, 1, 1, 2, 2, 2, 3, 3, 4, 4})
        .expectColumn("5", new int[]{1, 1, 2, 2, 3, 3, 4, 4, 5, 5})
        .expectColumn("6", new int[]{1, 1, 2, 2, 3, 3, 4, 4, 5, 6})
        .expectColumn("7", new int[]{1, 1, 2, 2, 3, 3, 4, 5, 6, 7})
        .expectColumn("8", new int[]{1, 1, 2, 2, 3, 4, 5, 6, 7, 8})
        .expectColumn("9", new int[]{1, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("10", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        .expectColumn("10292", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);
  }
}
