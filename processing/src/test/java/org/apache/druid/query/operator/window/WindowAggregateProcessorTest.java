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

package org.apache.druid.query.operator.window;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.query.rowsandcols.frame.MapOfColumnsRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowAggregateProcessorTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testAggregation()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
                new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
                ColumnType.STRING
            )
    );

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    WindowAggregateProcessor processor = new WindowAggregateProcessor(
        Arrays.asList(
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new LongSumAggregatorFactory("sumFromDouble", "doubleCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromDouble", "doubleCol")
        ),
        Arrays.asList(
            new LongMaxAggregatorFactory("cummMax", "intCol"),
            new DoubleSumAggregatorFactory("cummSum", "doubleCol")
        )
    );

    RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("sumFromLong", new long[]{45, 45, 45, 45, 45, 45, 45, 45, 45, 45})
        .expectColumn("sumFromDouble", new long[]{45, 45, 45, 45, 45, 45, 45, 45, 45, 45})
        .expectColumn("maxFromInt", new double[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("maxFromDouble", new double[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
        .expectColumn("cummMax", new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("cummSum", new double[]{0, 1, 3, 6, 10, 15, 21, 28, 36, 45});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);
  }

  @Test
  public void testValidateEquality()
  {
    WindowAggregateProcessor processor = new WindowAggregateProcessor(
        Arrays.asList(
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new LongSumAggregatorFactory("sumFromDouble", "doubleCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromDouble", "doubleCol")
        ),
        Arrays.asList(
            new LongMaxAggregatorFactory("cummMax", "intCol"),
            new DoubleSumAggregatorFactory("cummSum", "doubleCol")
        )
    );

    Assert.assertTrue(processor.validateEquivalent(processor));
    Assert.assertFalse(processor.validateEquivalent(new WindowRowNumberProcessor("bob")));
    Assert.assertFalse(processor.validateEquivalent(new WindowAggregateProcessor(processor.getAggregations(), null)));
    Assert.assertFalse(processor.validateEquivalent(
        new WindowAggregateProcessor(new ArrayList<>(), processor.getCumulativeAggregations())
    ));
    Assert.assertFalse(processor.validateEquivalent(new WindowAggregateProcessor(new ArrayList<>(), null)));
  }

}
