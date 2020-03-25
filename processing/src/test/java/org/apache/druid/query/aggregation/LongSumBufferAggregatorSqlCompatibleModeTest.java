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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateTestBase.TestColumn;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ListBasedSingleColumnCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class LongSumBufferAggregatorSqlCompatibleModeTest extends InitializedNullHandlingTest
{
  @ClassRule
  public static AssumingSqlCompatibleMode ASSUMING_SQL_COMPATIBLE_MODE = new AssumingSqlCompatibleMode();

  @Parameters
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{
            new LongSumAggregatorFactory(TestColumn.LONG_COLUMN.getName(), TestColumn.LONG_COLUMN.getName()),
            (LongFunction<Number>) val -> val
        },
        new Object[]{
            new LongSumAggregatorFactory(
                TestColumn.LONG_COLUMN.getName(),
                null,
                StringUtils.format("%s + 1", TestColumn.LONG_COLUMN.getName()),
                TestExprMacroTable.INSTANCE
            ),
            (LongFunction<Number>) val -> val + 1
        }
    );
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final LongSumAggregatorFactory aggregatorFactory;
  private final LongFunction<Number> expectedResultCalculator;

  private ByteBuffer buffer;

  public LongSumBufferAggregatorSqlCompatibleModeTest(
      LongSumAggregatorFactory aggregatorFactory,
      LongFunction<Number> expectedResultCalculator
  )
  {
    this.aggregatorFactory = aggregatorFactory;
    this.expectedResultCalculator = expectedResultCalculator;
  }

  @Before
  public void setup()
  {
    buffer = ByteBuffer.allocate(aggregatorFactory.getMaxIntermediateSizeWithNulls());
  }

  @Test
  public void testInit()
  {
    // write a garbage
    buffer.putLong(0, 10L);
    try (BufferAggregator aggregator = createAggregatorForValue(null)) {
      aggregator.init(buffer, 0);
      Assert.assertEquals(72057594037927936L, buffer.getLong(0));
    }
  }

  @Test
  public void testGet()
  {
    final long val = 1L;
    final Number expectedResult = expectedResultCalculator.apply(val);
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      Assert.assertEquals(expectedResult, aggregator.get(buffer, 0));
      Assert.assertEquals(expectedResult.longValue(), aggregator.getLong(buffer, 0));
      Assert.assertEquals(expectedResult.doubleValue(), aggregator.getDouble(buffer, 0), 0);
      Assert.assertEquals(expectedResult.floatValue(), aggregator.getFloat(buffer, 0), 0);
    }
  }

  @Test
  public void testIsNull()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(null)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      Assert.assertTrue(aggregator.isNull(buffer, 0));
      Assert.assertNull(aggregator.get(buffer, 0));
    }
  }

  @Test
  public void testGetLongWithNull()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(null)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Cannot return long for Null Value");
      aggregator.getLong(buffer, 0);
    }
  }

  @Test
  public void testGetDoubleWithNull()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(null)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Cannot return double for Null Value");
      aggregator.getDouble(buffer, 0);
    }
  }

  @Test
  public void testGetFloatWithNull()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(null)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Cannot return float for Null Value");
      aggregator.getFloat(buffer, 0);
    }
  }

  @Test
  public void testRelocate()
  {
    // relocate does nothing
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      // write some value
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);

      byte[] copy = new byte[buffer.array().length];
      System.arraycopy(buffer.array(), 0, copy, 0, copy.length);
      ByteBuffer originalBuffer = ByteBuffer.wrap(copy);
      ByteBuffer newBuffer = ByteBuffer.allocate(0);
      aggregator.relocate(0, 0, buffer, newBuffer);
      Assert.assertArrayEquals(originalBuffer.array(), buffer.array());
    }
  }

  @Test
  public void testInspectRuntimeShape()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      RecordingRuntimeShapeInspector runtimeShapeInspector = new RecordingRuntimeShapeInspector();
      aggregator.inspectRuntimeShape(runtimeShapeInspector);

      final List<String> expectedVisited = new ArrayList<>();
      if (aggregatorFactory.getExpression() == null) {
        expectedVisited.add("delegate");
        expectedVisited.add("selector");
        expectedVisited.add("nullSelector");
      } else {
        expectedVisited.add("delegate");
        expectedVisited.add("selector");
        expectedVisited.add("baseSelector");
        expectedVisited.add("expression");
        expectedVisited.add("bindings");
        expectedVisited.add("nullSelector");
      }
      final List<String> actualVisited = runtimeShapeInspector
          .getVisited()
          .stream()
          .map(pair -> pair.lhs)
          .collect(Collectors.toList());
      Assert.assertEquals(expectedVisited, actualVisited);
    }
  }

  private BufferAggregator createAggregatorForValue(@Nullable Long val)
  {
    ListBasedSingleColumnCursor<Long> cursor = new ListBasedSingleColumnCursor<>(
        Long.class,
        Collections.singletonList(val)
    );
    return aggregatorFactory.factorizeBuffered(cursor.getColumnSelectorFactory());
  }
}
