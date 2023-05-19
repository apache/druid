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
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WindowRankProcessorTest
{
  @Test
  public void testRankProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("vals", new IntArrayColumn(new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290}));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    final List<String> orderingCols = Collections.singletonList("vals");
    Processor processor = new ComposingProcessor(
        new WindowRankProcessor(orderingCols, "rank", false),
        new WindowRankProcessor(orderingCols, "rankAsPercent", true)
    );

    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("vals", new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290})
        .expectColumn("rank", new int[]{1, 2, 2, 4, 5, 6, 7, 7, 9, 9})
        .expectColumn(
            "rankAsPercent",
            new double[]{0.0, 1 / 9d, 1 / 9d, 3 / 9d, 4 / 9d, 5 / 9d, 6 / 9d, 6 / 9d, 8 / 9d, 8 / 9d}
        );

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);

  }

  @Test
  public void testRankSingle()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("vals", new IntArrayColumn(new int[]{7}));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    final List<String> orderingCols = Collections.singletonList("vals");
    Processor processor = new ComposingProcessor(
        new WindowRankProcessor(orderingCols, "rank", false),
        new WindowRankProcessor(orderingCols, "rankAsPercent", true)
    );

    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("vals", new int[]{7})
        .expectColumn("rank", new int[]{1})
        .expectColumn("rankAsPercent", new double[]{0.0});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);
  }
}
