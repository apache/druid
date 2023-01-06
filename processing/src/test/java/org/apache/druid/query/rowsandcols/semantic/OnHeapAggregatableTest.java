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
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

public class OnHeapAggregatableTest extends SemanticTestBase
{
  public OnHeapAggregatableTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testOnHeapAggregatable()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "incremented", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            "zeroesOut", new IntArrayColumn(new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        )
    ));

    OnHeapAggregatable agger = rac.as(OnHeapAggregatable.class);
    if (agger == null) {
      agger = new DefaultOnHeapAggregatable(rac);
    }

    final ArrayList<Object> results = agger.aggregateAll(Arrays.asList(
        new LongSumAggregatorFactory("incremented", "incremented"),
        new LongMaxAggregatorFactory("zeroesOutMax", "zeroesOut"),
        new LongMinAggregatorFactory("zeroesOutMin", "zeroesOut")
    ));

    Assert.assertEquals(3, results.size());
    Assert.assertEquals(55L, results.get(0));
    Assert.assertEquals(82L, results.get(1));
    Assert.assertEquals(-90L, results.get(2));
  }
}
