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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TypesTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testIs()
  {
    Assert.assertTrue(Types.is(ColumnType.LONG, ValueType.LONG));
    Assert.assertTrue(Types.is(ColumnType.DOUBLE, ValueType.DOUBLE));
    Assert.assertTrue(Types.is(ColumnType.FLOAT, ValueType.FLOAT));
    Assert.assertTrue(Types.is(ColumnType.STRING, ValueType.STRING));
    Assert.assertTrue(Types.is(ColumnType.LONG_ARRAY, ValueType.ARRAY));
    Assert.assertTrue(Types.is(ColumnType.LONG_ARRAY.getElementType(), ValueType.LONG));
    Assert.assertTrue(Types.is(ColumnType.DOUBLE_ARRAY, ValueType.ARRAY));
    Assert.assertTrue(Types.is(ColumnType.DOUBLE_ARRAY.getElementType(), ValueType.DOUBLE));
    Assert.assertTrue(Types.is(ColumnType.STRING_ARRAY, ValueType.ARRAY));
    Assert.assertTrue(Types.is(ColumnType.STRING_ARRAY.getElementType(), ValueType.STRING));
    Assert.assertTrue(Types.is(TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE, ValueType.COMPLEX));

    Assert.assertFalse(Types.is(ColumnType.LONG, ValueType.DOUBLE));
    Assert.assertFalse(Types.is(ColumnType.DOUBLE, ValueType.FLOAT));

    Assert.assertFalse(Types.is(null, ValueType.STRING));
    Assert.assertTrue(Types.isNullOr(null, ValueType.STRING));
  }

  @Test
  public void testNullOrAnyOf()
  {
    Assert.assertTrue(Types.isNullOrAnyOf(ColumnType.LONG, ValueType.STRING, ValueType.LONG, ValueType.DOUBLE));
    Assert.assertFalse(Types.isNullOrAnyOf(ColumnType.DOUBLE, ValueType.STRING, ValueType.LONG, ValueType.FLOAT));
    Assert.assertTrue(Types.isNullOrAnyOf(null, ValueType.STRING, ValueType.LONG, ValueType.FLOAT));
  }

  @Test
  public void testEither()
  {
    Assert.assertTrue(Types.either(ColumnType.LONG, ColumnType.DOUBLE, ValueType.DOUBLE));
    Assert.assertFalse(Types.either(ColumnType.LONG, ColumnType.STRING, ValueType.DOUBLE));
  }
}
