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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.expression.OperatorConversions.DefaultOperandTypeChecker;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@RunWith(Enclosed.class)
public class OperatorConversionsTest
{
  public static class DefaultOperandTypeCheckerTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetOperandCountRange()
    {
      SqlOperandTypeChecker typeChecker = new DefaultOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
          2,
          IntSets.EMPTY_SET,
          null
      );
      SqlOperandCountRange countRange = typeChecker.getOperandCountRange();
      Assert.assertEquals(2, countRange.getMin());
      Assert.assertEquals(3, countRange.getMax());
    }

    @Test
    public void testIsOptional()
    {
      SqlOperandTypeChecker typeChecker = new DefaultOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
          2,
          IntSets.EMPTY_SET,
          null
      );
      Assert.assertFalse(typeChecker.isOptional(0));
      Assert.assertFalse(typeChecker.isOptional(1));
      Assert.assertTrue(typeChecker.isOptional(2));
    }

    @Test
    public void testAllowFullOperands()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testAllowFullOperands")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(SqlTypeName.INTEGER, false),
                      new OperandSpec(SqlTypeName.DATE, false)
                  )
              ),
              true
          )
      );
    }

    @Test
    public void testRequiredOperandsOnly()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testRequiredOperandsOnly")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.INTEGER, false))
              ),
              true
          )
      );
    }

    @Test
    public void testLiteralOperandCheckLiteral()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testLiteralOperandCheckLiteral")
          .operandTypes(SqlTypeFamily.INTEGER)
          .requiredOperands(1)
          .literalOperands(0)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertFalse(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.INTEGER, false))
              ),
              false
          )
      );
    }

    @Test
    public void testLiteralOperandCheckLiteralThrow()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testLiteralOperandCheckLiteralThrow")
          .operandTypes(SqlTypeFamily.INTEGER)
          .requiredOperands(1)
          .literalOperands(0)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      expectedException.expect(CalciteContextException.class);
      expectedException.expectMessage("Argument to function 'testLiteralOperandCheckLiteralThrow' must be a literal");
      typeChecker.checkOperandTypes(
          mockCallBinding(
              function,
              ImmutableList.of(new OperandSpec(SqlTypeName.INTEGER, false))
          ),
          true
      );
    }

    @Test
    public void testAnyTypeOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testAnyTypeOperand")
          .operandTypes(SqlTypeFamily.ANY)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.DISTINCT, false))
              ),
              true
          )
      );
    }

    @Test
    public void testImplicitlyCastableFromDatetimeFamilyToTimestamp()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testCastableFromDatetimeFamilyToTimestamp")
          .operandTypes(SqlTypeFamily.TIMESTAMP)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.DATE, false))
              ),
              true
          )
      );
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.TIME, false))
              ),
              true
          )
      );
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.TIMESTAMP, false))
              ),
              true
          )
      );
    }

    @Test
    public void testNullForNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(SqlTypeName.VARCHAR, false),
                      new OperandSpec(SqlTypeName.NULL, false)
                  )
              ),
              true
          )
      );
    }

    @Test
    public void testNullLiteralForNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullLiteralForNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assert.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(SqlTypeName.VARCHAR, false),
                      new OperandSpec(SqlTypeName.NULL, true)
                  )
              ),
              true
          )
      );
    }

    @Test
    public void testNullForNonNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNonNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      expectedException.expect(CalciteContextException.class);
      expectedException.expectMessage(
          "Exception in test for operator[testNullForNonNullableOperand]: Illegal use of 'NULL'"
      );
      typeChecker.checkOperandTypes(
          mockCallBinding(
              function,
              ImmutableList.of(
                  new OperandSpec(SqlTypeName.NULL, false),
                  new OperandSpec(SqlTypeName.INTERVAL_HOUR, false)
              )
          ),
          true
      );
    }

    @Test
    public void testNullLiteralForNonNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullLiteralForNonNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperands(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      expectedException.expect(CalciteContextException.class);
      expectedException.expectMessage(
          "Exception in test for operator[testNullLiteralForNonNullableOperand]: Illegal use of 'NULL'"
      );
      typeChecker.checkOperandTypes(
          mockCallBinding(
              function,
              ImmutableList.of(
                  new OperandSpec(SqlTypeName.NULL, true),
                  new OperandSpec(SqlTypeName.INTERVAL_HOUR, false)
              )
          ),
          true
      );
    }

    @Test
    public void testNonCastableType()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNonCastableType")
          .operandTypes(SqlTypeFamily.CURSOR, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperands(2)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      expectedException.expect(CalciteContextException.class);
      expectedException.expectMessage(
          "Exception in test for operator[testNonCastableType]: Cannot apply 'testNonCastableType' to arguments of type"
      );
      typeChecker.checkOperandTypes(
          mockCallBinding(
              function,
              ImmutableList.of(
                  new OperandSpec(SqlTypeName.INTEGER, true),
                  new OperandSpec(SqlTypeName.INTERVAL_HOUR, false)
              )
          ),
          true
      );
    }

    private static SqlCallBinding mockCallBinding(
        SqlFunction function,
        List<OperandSpec> actualOperands
    )
    {
      SqlValidator validator = Mockito.mock(SqlValidator.class);
      List<SqlNode> operands = new ArrayList<>(actualOperands.size());
      for (OperandSpec operand : actualOperands) {
        final SqlNode node;
        if (operand.isLiteral) {
          node = Mockito.mock(SqlLiteral.class);
        } else {
          node = Mockito.mock(SqlNode.class);
        }
        RelDataType relDataType = Mockito.mock(RelDataType.class);
        Mockito.when(validator.deriveType(ArgumentMatchers.any(), ArgumentMatchers.eq(node)))
               .thenReturn(relDataType);
        Mockito.when(relDataType.getSqlTypeName()).thenReturn(operand.type);
        operands.add(node);
      }
      SqlParserPos pos = Mockito.mock(SqlParserPos.class);
      Mockito.when(pos.plusAll(ArgumentMatchers.any(Collection.class)))
             .thenReturn(pos);
      SqlCallBinding callBinding = new SqlCallBinding(
          validator,
          Mockito.mock(SqlValidatorScope.class),
          function.createCall(pos, operands)
      );

      Mockito.when(validator.newValidationError(ArgumentMatchers.any(), ArgumentMatchers.any()))
             .thenAnswer((Answer<CalciteContextException>) invocationOnMock -> new CalciteContextException(
                 StringUtils.format("Exception in test for operator[%s]", function.getName()),
                 invocationOnMock.getArgument(1, ExInst.class).ex()
             ));
      return callBinding;
    }

    private static class OperandSpec
    {
      private final SqlTypeName type;
      private final boolean isLiteral;

      private OperandSpec(SqlTypeName type, boolean isLiteral)
      {
        this.type = type;
        this.isLiteral = isLiteral;
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class ImplicitCastabilityTest
  {
    @Parameters(name = "Cast from {0}")
    public static List<Object[]> parameters()
    {
      final List<Object[]> params = new ArrayList<>();
      final Set<SqlTypeName> addedTypes = new HashSet<>();

      final List<SqlTypeFamily> numericFamilies = ImmutableList.of(
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.APPROXIMATE_NUMERIC,
          SqlTypeFamily.EXACT_NUMERIC,
          SqlTypeFamily.DECIMAL,
          SqlTypeFamily.INTEGER
      );
      final List<SqlTypeFamily> dateTimeFamilies = ImmutableList.of(
          SqlTypeFamily.DATE,
          SqlTypeFamily.TIME,
          SqlTypeFamily.TIMESTAMP,
          SqlTypeFamily.DATETIME
      );
      final List<SqlTypeFamily> intervalFamilies = ImmutableList.of(
          SqlTypeFamily.INTERVAL_YEAR_MONTH,
          SqlTypeFamily.INTERVAL_DAY_TIME,
          SqlTypeFamily.DATETIME_INTERVAL
      );

      params.add(new Object[]{SqlTypeName.BOOLEAN, ImmutableSet.of(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY)});
      addedTypes.add(SqlTypeName.BOOLEAN);

      SqlTypeName.INT_TYPES.forEach(type -> {
        Set<SqlTypeFamily> f = new HashSet<>(numericFamilies);
        f.add(SqlTypeFamily.ANY);
        params.add(new Object[]{type, f});
        addedTypes.add(type);
      });

      Stream.concat(Stream.of(SqlTypeName.DECIMAL), SqlTypeName.APPROX_TYPES.stream()).forEach(type -> {
        Set<SqlTypeFamily> f = new HashSet<>(numericFamilies);
        f.add(SqlTypeFamily.ANY);
        params.add(new Object[]{type, f});
        addedTypes.add(type);
      });

      SqlTypeName.DATETIME_TYPES.forEach(type -> {
        Set<SqlTypeFamily> f = new HashSet<>(dateTimeFamilies);
        f.add(SqlTypeFamily.ANY);
        params.add(new Object[]{type, f});
        addedTypes.add(type);
      });

      SqlTypeName.INTERVAL_TYPES.forEach(type -> {
        Set<SqlTypeFamily> f = new HashSet<>(intervalFamilies);
        f.add(SqlTypeFamily.ANY);
        params.add(new Object[]{type, f});
        addedTypes.add(type);
      });

      Stream.of(SqlTypeName.NULL, SqlTypeName.ANY).forEach(type -> {
        Set<SqlTypeFamily> f = EnumSet.allOf(SqlTypeFamily.class);
        params.add(new Object[]{type, f});
        addedTypes.add(type);
      });

      SqlTypeName.STRING_TYPES.forEach(type -> {
        Set<SqlTypeFamily> f = new HashSet<>();
        f.add(type.getFamily());
        f.add(SqlTypeFamily.STRING);
        f.add(SqlTypeFamily.ANY);
        params.add(new Object[]{type, f});
        addedTypes.add(type);
      });

      // not castable to ANY because Calcite thinks the ANY type family doesn't include these.
      Stream.of(
          SqlTypeName.ARRAY,
          SqlTypeName.MAP,
          SqlTypeName.OTHER,
          SqlTypeName.GEOMETRY
      ).forEach(type -> {
        if (!addedTypes.contains(type)) {
          Set<SqlTypeFamily> f = new HashSet<>();
          f.add(type.getFamily());
          params.add(new Object[]{type, f});
          addedTypes.add(type);
        }
      });

      for (SqlTypeName type : SqlTypeName.values()) {
        if (!addedTypes.contains(type)
            // Skip dynamic_star because it returns ANY as its family
            // but the ANY family doesn't return it in getTypeNames() (is it expelled from the family..?)
            // which makes the unit test failed.
            && type != SqlTypeName.DYNAMIC_STAR) {
          Set<SqlTypeFamily> f = new HashSet<>();
          f.add(type.getFamily());
          f.add(SqlTypeFamily.ANY);
          params.add(new Object[]{type, f});
          addedTypes.add(type);
        }
      }
      return params;
    }

    private final SqlTypeName fromType;
    private final Set<SqlTypeFamily> castableToTypeFamily;

    public ImplicitCastabilityTest(SqlTypeName fromType, Set<SqlTypeFamily> castableToTypeFamily)
    {
      this.fromType = fromType;
      this.castableToTypeFamily = castableToTypeFamily;
    }

    @Test
    public void testCastableImplicitly()
    {
      for (SqlTypeFamily family : SqlTypeFamily.values()) {
        if (castableToTypeFamily.contains(family)) {
          Assert.assertTrue(
              StringUtils.format("Expected to be able to cast from[%s] to[%s], but cannot", fromType, family),
              DefaultOperandTypeChecker.isCastableImplicitly(fromType, family)
          );
        } else {
          Assert.assertFalse(
              StringUtils.format("Expected to not be able to cast from[%s] to[%s], but can", fromType, family),
              DefaultOperandTypeChecker.isCastableImplicitly(fromType, family)
          );
        }
      }
    }
  }
}
