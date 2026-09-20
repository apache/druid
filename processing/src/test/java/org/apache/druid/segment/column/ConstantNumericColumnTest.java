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

package org.apache.druid.segment.column;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConstantNumericColumnTest
{
  @Test
  void testLongSelectorReturnsBoxedValueNotExprEval()
  {
    final ColumnValueSelector<?> selector =
        new ConstantNumericColumn(ColumnType.LONG, 42L, 3).makeColumnValueSelector(null);
    // The single-group path reads this selector directly (no unwrapping wrapper), so getObject() must return the boxed
    // value like a physical numeric column -- not the ExprEval that ConstantExprEvalSelector would expose.
    Assertions.assertFalse(selector.getObject() instanceof ExprEval);
    Assertions.assertEquals(42L, selector.getObject());
    Assertions.assertEquals(Object.class, selector.classOfObject());
    Assertions.assertEquals(42L, selector.getLong());
    Assertions.assertEquals(42.0, selector.getDouble(), 0.0);
    Assertions.assertFalse(selector.isNull());
  }

  @Test
  void testDoubleSelector()
  {
    final ColumnValueSelector<?> selector =
        new ConstantNumericColumn(ColumnType.DOUBLE, 1.5, 1).makeColumnValueSelector(null);
    Assertions.assertEquals(1.5, selector.getObject());
    Assertions.assertEquals(1.5, selector.getDouble(), 0.0);
    Assertions.assertEquals(1L, selector.getLong());
    Assertions.assertFalse(selector.isNull());
  }

  @Test
  void testFloatSelector()
  {
    final ColumnValueSelector<?> selector =
        new ConstantNumericColumn(ColumnType.FLOAT, 2.5f, 1).makeColumnValueSelector(null);
    Assertions.assertEquals(2.5f, selector.getObject());
    Assertions.assertEquals(2.5f, selector.getFloat(), 0.0f);
    Assertions.assertFalse(selector.isNull());
  }

  @Test
  void testNullValue()
  {
    final ColumnValueSelector<?> selector =
        new ConstantNumericColumn(ColumnType.LONG, null, 1).makeColumnValueSelector(null);
    Assertions.assertTrue(selector.isNull());
    Assertions.assertNull(selector.getObject());
    Assertions.assertEquals(0L, selector.getLong());
  }
}
