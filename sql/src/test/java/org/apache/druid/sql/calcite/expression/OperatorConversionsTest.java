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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.planner.DruidTypeCoercion;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class OperatorConversionsTest
{
  private static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);
  private static final ColumnType ALLOWED_COMPLEX_TYPE = ColumnType.ofComplex("allowedComplex");
  private static final ColumnType OTHER_COMPLEX_TYPE = ColumnType.ofComplex("otherComplex");

  @Nested
  public class DefaultOperandTypeCheckerTest
  {
    @Test
    public void testGetOperandCountRange()
    {
      SqlOperandTypeChecker typeChecker = DefaultOperandTypeChecker
          .builder()
          .operandNames()
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
          .requiredOperandCount(2)
          .literalOperands()
          .build();
      SqlOperandCountRange countRange = typeChecker.getOperandCountRange();
      Assertions.assertEquals(2, countRange.getMin());
      Assertions.assertEquals(3, countRange.getMax());
    }

    @Test
    public void testIsOptional()
    {
      SqlOperandTypeChecker typeChecker = DefaultOperandTypeChecker
          .builder()
          .operandNames()
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
          .requiredOperandCount(2)
          .literalOperands()
          .build();
      Assertions.assertFalse(typeChecker.isOptional(0));
      Assertions.assertFalse(typeChecker.isOptional(1));
      Assertions.assertTrue(typeChecker.isOptional(2));
    }

    @Test
    public void testAllowFullOperands()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testAllowFullOperands")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
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
          .operandTypeChecker(DefaultOperandTypeChecker.builder().operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE).requiredOperandCount(1).build())
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
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
          .requiredOperandCount(1)
          .literalOperands(0)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertFalse(
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
          .requiredOperandCount(1)
          .literalOperands(0)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertThrowsWithMessage(
          () -> typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.INTEGER, false))
              ),
              true
          ),
          "Argument to function 'testLiteralOperandCheckLiteralThrow' must be a literal"
      );
    }

    @Test
    public void testAnyTypeOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testAnyTypeOperand")
          .operandTypes(SqlTypeFamily.ANY)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
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
    public void testCastableFromDateTimestampToDatetimeFamily()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testCastableFromDatetimeFamilyToTimestamp")
          .operandTypes(SqlTypeFamily.DATETIME)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.DATE, false))
              ),
              true
          )
      );
      Assertions.assertTrue(
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
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
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
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
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
    public void testNullForNullableOperandNonNullOutput()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNullableNonnull")
          .operandTypes(SqlTypeFamily.CHARACTER)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      SqlCallBinding binding = mockCallBinding(
          function,
          ImmutableList.of(
              new OperandSpec(SqlTypeName.CHAR, false, true)
          )
      );
      Assertions.assertTrue(typeChecker.checkOperandTypes(binding, true));
      RelDataType returnType = function.getReturnTypeInference().inferReturnType(binding);
      Assertions.assertFalse(returnType.isNullable());
    }

    @Test
    public void testNullForNullableOperandCascadeNullOutput()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNullableCascade")
          .operandTypes(SqlTypeFamily.CHARACTER)
          .requiredOperandCount(1)
          .returnTypeCascadeNullable(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      SqlCallBinding binding = mockCallBinding(
          function,
          ImmutableList.of(
              new OperandSpec(SqlTypeName.CHAR, false, true)
          )
      );
      Assertions.assertTrue(typeChecker.checkOperandTypes(binding, true));
      RelDataType returnType = function.getReturnTypeInference().inferReturnType(binding);
      Assertions.assertTrue(returnType.isNullable());
    }

    @Test
    public void testNullForNullableOperandAlwaysNullableOutput()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNullableNonnull")
          .operandTypes(SqlTypeFamily.CHARACTER)
          .requiredOperandCount(1)
          .returnTypeNullable(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      SqlCallBinding binding = mockCallBinding(
          function,
          ImmutableList.of(
              new OperandSpec(SqlTypeName.CHAR, false, false)
          )
      );
      Assertions.assertTrue(typeChecker.checkOperandTypes(binding, true));
      RelDataType returnType = function.getReturnTypeInference().inferReturnType(binding);
      Assertions.assertTrue(returnType.isNullable());
    }

    @Test
    public void testNullForNonNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNonNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertThrowsWithMessage(
          () -> typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(SqlTypeName.NULL, false),
                      new OperandSpec(SqlTypeName.INTERVAL_HOUR, false)
                  )
              ),
              true
          ),
          "Exception in test for operator[testNullForNonNullableOperand]: Illegal use of 'NULL'"
      );
    }

    @Test
    public void testNullLiteralForNonNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullLiteralForNonNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertThrowsWithMessage(
          () -> typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(SqlTypeName.NULL, true),
                      new OperandSpec(SqlTypeName.INTERVAL_HOUR, false)
                  )
              ),
              true
          ),
          "Exception in test for operator[testNullLiteralForNonNullableOperand]: Illegal use of 'NULL'"
      );
    }

    @Test
    public void testNonCastableType()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNonCastableType")
          .operandTypes(SqlTypeFamily.CURSOR, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperandCount(2)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertThrowsWithMessage(
          () -> typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(SqlTypeName.INTEGER, true),
                      new OperandSpec(SqlTypeName.INTERVAL_HOUR, false)
                  )
              ),
              true
          ),
          "Exception in test for operator[testNonCastableType]: Cannot apply 'testNonCastableType' to arguments of type"
      );
    }

    @Test
    public void testAllowedComplexTypeOperand()
    {
      SqlFunction function = numericOperandsFunction("testAllowedComplexTypeOperand");
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      Assertions.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(RowSignatures.makeComplexType(TYPE_FACTORY, ALLOWED_COMPLEX_TYPE, false)),
                      new OperandSpec(SqlTypeName.DOUBLE, false)
                  )
              ),
              true
          )
      );
    }

    @Test
    public void testOtherComplexTypeOperand()
    {
      SqlFunction function = numericOperandsFunction("testOtherComplexTypeOperand");
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertThrowsWithMessage(
          () -> typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(RowSignatures.makeComplexType(TYPE_FACTORY, OTHER_COMPLEX_TYPE, true)),
                      new OperandSpec(SqlTypeName.DOUBLE, false)
                  )
              ),
              true
          ),
          "Cannot apply 'testOtherComplexTypeOperand' to arguments of type"
      );
    }

    @Test
    public void testOperandWithoutOptions()
    {
      // Only the first operand allows complex types and string coercion, so the second one is checked as usual
      SqlFunction function = numericOperandsFunction("testOperandWithoutOptions");
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      final List<OperandSpec> invalidSecondOperands = ImmutableList.of(
          new OperandSpec(RowSignatures.makeComplexType(TYPE_FACTORY, ALLOWED_COMPLEX_TYPE, true)),
          new OperandSpec(SqlTypeName.VARCHAR, false),
          new OperandSpec(SqlLiteral.createCharString("abc", SqlParserPos.ZERO))
      );
      for (OperandSpec secondOperand : invalidSecondOperands) {
        final TypeCoercion typeCoercion = Mockito.mock(DruidTypeCoercion.class);
        assertThrowsWithMessage(
            () -> typeChecker.checkOperandTypes(
                mockCallBinding(
                    function,
                    ImmutableList.of(new OperandSpec(SqlTypeName.DOUBLE, false), secondOperand),
                    typeCoercion
                ),
                true
            ),
            "Cannot apply 'testOperandWithoutOptions' to arguments of type"
        );
        Mockito.verifyNoInteractions(typeCoercion);
      }
    }

    @Test
    public void testNonNumericStringLiteralOnCoercibleOperand()
    {
      SqlFunction function = numericOperandsFunction("testNonNumericStringLiteralOnCoercibleOperand");
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      final TypeCoercion typeCoercion = Mockito.mock(DruidTypeCoercion.class);
      final List<OperandSpec> operands = ImmutableList.of(
          new OperandSpec(SqlLiteral.createCharString("m1", SqlParserPos.ZERO)),
          new OperandSpec(SqlTypeName.DOUBLE, false)
      );

      final DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> typeChecker.checkOperandTypes(mockCallBinding(function, operands, typeCoercion), true)
      );
      Assertions.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
      Assertions.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
      Assertions.assertEquals(
          "Function [testNonNumericStringLiteralOnCoercibleOperand] argument [x] must be numeric, got string literal"
          + " ['m1']. If you meant to refer to a column, use double quotes.",
          e.getMessage()
      );

      Assertions.assertFalse(typeChecker.checkOperandTypes(mockCallBinding(function, operands, typeCoercion), false));
      Mockito.verifyNoInteractions(typeCoercion);
    }

    @Test
    public void testNumericStringLiteralOnCoercibleOperand()
    {
      assertCoercedToDouble(
          "testNumericStringLiteralOnCoercibleOperand",
          new OperandSpec(SqlLiteral.createCharString("1.5", SqlParserPos.ZERO))
      );
    }

    @Test
    public void testStringOperandOnCoercibleOperand()
    {
      assertCoercedToDouble("testStringOperandOnCoercibleOperand", new OperandSpec(SqlTypeName.VARCHAR, false, true));
    }

    @Test
    public void testBooleanOperandOnCoercibleOperand()
    {
      SqlFunction function = numericOperandsFunction("testBooleanOperandOnCoercibleOperand");
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      final TypeCoercion typeCoercion = Mockito.mock(DruidTypeCoercion.class);
      assertThrowsWithMessage(
          () -> typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.BOOLEAN, false), new OperandSpec(SqlTypeName.DOUBLE, false)),
                  typeCoercion
              ),
              true
          ),
          "Cannot apply 'testBooleanOperandOnCoercibleOperand' to arguments of type"
      );
      Mockito.verifyNoInteractions(typeCoercion);
    }

    @Test
    public void testTimestampOperandOnCoercibleOperand()
    {
      SqlFunction function = numericOperandsFunction("testTimestampOperandOnCoercibleOperand");
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      final TypeCoercion typeCoercion = Mockito.mock(DruidTypeCoercion.class);
      Assertions.assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(
                      new OperandSpec(TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP)),
                      new OperandSpec(SqlTypeName.DOUBLE, false)
                  ),
                  typeCoercion
              ),
              true
          )
      );
      Mockito.verifyNoInteractions(typeCoercion);
    }

    private void assertCoercedToDouble(String functionName, OperandSpec firstOperand)
    {
      SqlFunction function = numericOperandsFunction(functionName);
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      final DruidTypeCoercion typeCoercion = Mockito.mock(DruidTypeCoercion.class);
      Mockito.when(
          typeCoercion.coerceOperand(
              ArgumentMatchers.any(),
              ArgumentMatchers.any(),
              ArgumentMatchers.anyInt(),
              ArgumentMatchers.any()
          )
      ).thenReturn(true);
      final SqlCallBinding callBinding = mockCallBinding(
          function,
          ImmutableList.of(firstOperand, new OperandSpec(SqlTypeName.DOUBLE, false)),
          typeCoercion
      );

      Assertions.assertTrue(typeChecker.checkOperandTypes(callBinding, true));
      Mockito.verify(typeCoercion).coerceOperand(
          ArgumentMatchers.eq(callBinding.getScope()),
          ArgumentMatchers.same(callBinding.getCall()),
          ArgumentMatchers.eq(0),
          ArgumentMatchers.argThat(type -> type.getSqlTypeName() == SqlTypeName.DOUBLE)
      );
      Mockito.verifyNoMoreInteractions(typeCoercion);
    }

    /**
     * Function with two numeric operands, where only the first one allows {@link #ALLOWED_COMPLEX_TYPE} and coercion
     * of strings.
     */
    private SqlFunction numericOperandsFunction(String name)
    {
      return OperatorConversions
          .operatorBuilder(name)
          .operandNames("x", "y")
          .operandTypes(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
          .operandComplexTypes(0, ALLOWED_COMPLEX_TYPE)
          .coercibleOperands(0)
          .returnTypeNonNull(SqlTypeName.DOUBLE)
          .build();
    }

    private static void assertThrowsWithMessage(Runnable runnable, String expectedMessage)
    {
      final CalciteContextException e = Assertions.assertThrows(CalciteContextException.class, runnable::run);
      Assertions.assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
    }

    @Test
    public void testSignatureWithNames()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testSignatureWithNames")
          .operandNames("x", "y", "z")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE, SqlTypeFamily.ANY)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();

      Assertions.assertEquals(
          "'testSignatureWithNames(<x>, [<y>, [<z>]])'",
          typeChecker.getAllowedSignatures(function, function.getName())
      );
    }

    @Test
    public void testSignatureWithoutNames()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testSignatureWithoutNames")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE, SqlTypeFamily.ANY)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();

      Assertions.assertEquals(
          "'testSignatureWithoutNames(<INTEGER>, [<DATE>, [<ANY>]])'",
          typeChecker.getAllowedSignatures(function, function.getName())
      );
    }

    private static SqlCallBinding mockCallBinding(
        SqlFunction function,
        List<OperandSpec> actualOperands
    )
    {
      return mockCallBinding(function, actualOperands, null);
    }

    private static SqlCallBinding mockCallBinding(
        SqlFunction function,
        List<OperandSpec> actualOperands,
        @Nullable TypeCoercion typeCoercion
    )
    {
      SqlValidator validator = Mockito.mock(SqlValidator.class);
      Mockito.when(validator.getTypeFactory()).thenReturn(TYPE_FACTORY);
      Mockito.when(validator.config()).thenReturn(SqlValidator.Config.DEFAULT);
      Mockito.when(validator.getTypeCoercion()).thenReturn(typeCoercion);
      List<SqlNode> operands = new ArrayList<>(actualOperands.size());
      for (OperandSpec operand : actualOperands) {
        if (operand.node != null || operand.relDataType != null) {
          final SqlNode node = operand.node != null ? operand.node : nonLiteralNode();
          final RelDataType relDataType = operand.relDataType != null
                                          ? operand.relDataType
                                          : ((SqlLiteral) node).createSqlType(TYPE_FACTORY);
          Mockito.when(validator.deriveType(ArgumentMatchers.any(), ArgumentMatchers.same(node)))
                 .thenReturn(relDataType);
          operands.add(node);
          continue;
        }

        final SqlNode node;
        if (operand.isLiteral) {
          node = Mockito.mock(SqlLiteral.class);
          Mockito.when(node.getKind()).thenReturn(SqlKind.LITERAL);
        } else {
          node = nonLiteralNode();
        }
        RelDataType relDataType = Mockito.mock(RelDataType.class);

        if (operand.isNullable) {
          Mockito.when(relDataType.isNullable()).thenReturn(true);
        } else {
          Mockito.when(relDataType.isNullable()).thenReturn(false);
        }
        Mockito.when(validator.deriveType(ArgumentMatchers.any(), ArgumentMatchers.eq(node)))
               .thenReturn(relDataType);
        Mockito.when(relDataType.getSqlTypeName()).thenReturn(operand.type);


        operands.add(node);
      }
      SqlParserPos pos = Mockito.mock(SqlParserPos.class);

      Mockito.when(pos.plusAll((SqlNode[]) ArgumentMatchers.any()))
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

    private static SqlNode nonLiteralNode()
    {
      final SqlNode node = Mockito.mock(SqlNode.class);
      // Setting this as SqlUtil.isLiteral makes a call on
      // node.getKind() and without this change would
      // return a NPE
      Mockito.when(node.getKind()).thenReturn(SqlKind.OTHER_FUNCTION);
      return node;
    }

    private static class OperandSpec
    {
      private final SqlTypeName type;
      private final boolean isLiteral;
      private final boolean isNullable;

      /**
       * Actual node of the operand, used instead of a mock if not null.
       */
      @Nullable
      private final SqlNode node;

      /**
       * Actual derived type of the operand, used instead of a mock if not null.
       */
      @Nullable
      private final RelDataType relDataType;

      private OperandSpec(SqlTypeName type, boolean isLiteral)
      {
        this(type, isLiteral, type == SqlTypeName.NULL);
      }

      private OperandSpec(SqlTypeName type, boolean isLiteral, boolean isNullable)
      {
        this.type = type;
        this.isLiteral = isLiteral;
        this.isNullable = isNullable;
        this.node = null;
        this.relDataType = null;
      }

      private OperandSpec(SqlLiteral node)
      {
        this.type = null;
        this.isLiteral = true;
        this.isNullable = false;
        this.node = node;
        this.relDataType = null;
      }

      private OperandSpec(RelDataType relDataType)
      {
        this.type = relDataType.getSqlTypeName();
        this.isLiteral = false;
        this.isNullable = relDataType.isNullable();
        this.node = null;
        this.relDataType = relDataType;
      }
    }
  }
}
