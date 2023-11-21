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

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.rowsandcols.AsOnlyTestRowsAndColumns;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.NoAsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.semantic.FramedOnHeapAggregatable;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class WindowFramedAggregateProcessorTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testIsPassThruWhenRACReturnsSemanticInterface()
  {
    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, false, 0, null);
    final AggregatorFactory[] theAggs = {
        new LongMaxAggregatorFactory("cummMax", "intCol"),
        new DoubleSumAggregatorFactory("cummSum", "doubleCol")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(ImmutableMap.of(
        "yay", new IntArrayColumn(new int[]{1, 2, 3})
    ));

    final RowsAndColumns processed = proc.process(new AsOnlyTestRowsAndColumns()
    {
      @Override
      public <T> T as(Class<T> clazz)
      {
        Assert.assertEquals(clazz, FramedOnHeapAggregatable.class);
        return (T) (FramedOnHeapAggregatable) (frame, aggFactories) -> {
          Assert.assertEquals(theFrame, frame);
          Assert.assertArrayEquals(theAggs, aggFactories);
          return rac;
        };
      }
    });

    Assert.assertSame(rac, processed);
  }

  @Test
  public void testDoesStuffWhenNoSemanticInterfacesAvailable()
  {
    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, false, 0, null);
    final AggregatorFactory[] theAggs = {
        new LongSumAggregatorFactory("sum", "intCol")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(ImmutableMap.of(
        "intCol", new IntArrayColumn(new int[]{1, 2, 3})
    ));

    final RowsAndColumns processed = proc.process(new NoAsRowsAndColumns(rac));

    new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{1, 2, 3})
        .expectColumn("sum", new int[]{1, 3, 6})
        .allColumnsRegistered()
        .validate(processed);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WindowFramedAggregateProcessor.class)
        .usingGetClass()
        .verify();
  }
}
