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

package org.apache.druid.sql.calcite.expression;

import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OperandTypeTest
{
  private static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);
  private static final ColumnType COMPLEX_TYPE = ColumnType.ofComplex("someComplex");

  @Test
  public void testOf()
  {
    final OperandType type = OperandType.of(SqlTypeFamily.STRING);
    Assertions.assertEquals(SqlTypeFamily.STRING, type.getFamily());
    Assertions.assertFalse(type.isCoerceStrings());
    Assertions.assertFalse(type.isNullable());
    Assertions.assertFalse(type.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, COMPLEX_TYPE, false)));
    Assertions.assertEquals("STRING", type.toString());
  }

  @Test
  public void testNullable()
  {
    final OperandType numeric = OperandType.numeric().orComplex(COMPLEX_TYPE).coerceStrings();
    final OperandType type = numeric.nullable();

    Assertions.assertTrue(type.isNullable());
    Assertions.assertFalse(numeric.isNullable());
    Assertions.assertTrue(type.isCoerceStrings());
    Assertions.assertTrue(type.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, COMPLEX_TYPE, false)));

    // nullable() is kept by the other methods.
    Assertions.assertTrue(OperandType.numeric().nullable().orComplex(COMPLEX_TYPE).coerceStrings().isNullable());
  }

  @Test
  public void testOrComplex()
  {
    final OperandType numeric = OperandType.numeric();
    final OperandType type = numeric.orComplex(COMPLEX_TYPE);

    Assertions.assertEquals(SqlTypeFamily.NUMERIC, type.getFamily());
    Assertions.assertTrue(type.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, COMPLEX_TYPE, false)));
    Assertions.assertTrue(type.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, COMPLEX_TYPE, true)));
    Assertions.assertFalse(
        type.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, ColumnType.ofComplex("other"), false))
    );
    Assertions.assertFalse(type.isAllowedComplexType(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE)));
    Assertions.assertEquals("NUMERIC | COMPLEX<someComplex>", type.toString());

    // The original operand type is not changed.
    Assertions.assertFalse(
        numeric.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, COMPLEX_TYPE, false))
    );
  }

  @Test
  public void testOrComplexOnAnyOperand()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> OperandType.of(SqlTypeFamily.ANY).orComplex(COMPLEX_TYPE)
    );
    Assertions.assertEquals("orComplex() does not apply to [ANY] operands", e.getMessage());
  }

  @Test
  public void testCoerceStrings()
  {
    final OperandType numeric = OperandType.numeric().orComplex(COMPLEX_TYPE);
    final OperandType type = numeric.coerceStrings();

    Assertions.assertTrue(type.isCoerceStrings());
    Assertions.assertFalse(numeric.isCoerceStrings());
    Assertions.assertTrue(type.isAllowedComplexType(RowSignatures.makeComplexType(TYPE_FACTORY, COMPLEX_TYPE, false)));
  }

  @Test
  public void testCoerceStringsOnNonNumericOperand()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> OperandType.of(SqlTypeFamily.STRING).coerceStrings()
    );
    Assertions.assertEquals("coerceStrings() only applies to [NUMERIC] operands, got [STRING]", e.getMessage());
  }
}
