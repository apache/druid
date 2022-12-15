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

import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowDenseRankProcessorTest
{
  @Test
  public void testDenseRankProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("vals", new IntArrayColumn(new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290}));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    Processor processor = new WindowDenseRankProcessor(Collections.singletonList("vals"), "DenseRank");

    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("vals", new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290})
        .expectColumn("DenseRank", new int[]{1, 2, 2, 3, 4, 5, 6, 6, 7, 7});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);

  }
}
