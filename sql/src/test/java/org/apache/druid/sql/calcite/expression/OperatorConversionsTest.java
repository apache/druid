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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;



public class OperatorConversionsTest
{
  @Nested
  class DefaultOperandTypeCheckerTest
  {

    @Test
    void getOperandCountRange()
    {
      SqlOperandTypeChecker typeChecker = DefaultOperandTypeChecker
          .builder()
          .operandNames()
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
          .requiredOperandCount(2)
          .literalOperands()
          .build();
      SqlOperandCountRange countRange = typeChecker.getOperandCountRange();
      assertEquals(2, countRange.getMin());
      assertEquals(3, countRange.getMax());
    }

    @Test
    void isOptional()
    {
      SqlOperandTypeChecker typeChecker = DefaultOperandTypeChecker
          .builder()
          .operandNames()
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
          .requiredOperandCount(2)
          .literalOperands()
          .build();
      assertFalse(typeChecker.isOptional(0));
      assertFalse(typeChecker.isOptional(1));
      assertTrue(typeChecker.isOptional(2));
    }

    @Test
    void allowFullOperands()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testAllowFullOperands")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertTrue(
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
    void requiredOperandsOnly()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testRequiredOperandsOnly")
          .operandTypeChecker(DefaultOperandTypeChecker.builder().operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE).requiredOperandCount(1).build())
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertTrue(
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
    void literalOperandCheckLiteral()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testLiteralOperandCheckLiteral")
          .operandTypes(SqlTypeFamily.INTEGER)
          .requiredOperandCount(1)
          .literalOperands(0)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertFalse(
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
    void literalOperandCheckLiteralThrow()
    {
      Throwable exception = assertThrows(CalciteContextException.class, () -> {
        SqlFunction function = OperatorConversions
            .operatorBuilder("testLiteralOperandCheckLiteralThrow")
            .operandTypes(SqlTypeFamily.INTEGER)
            .requiredOperandCount(1)
            .literalOperands(0)
            .returnTypeNonNull(SqlTypeName.CHAR)
            .build();
        SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
        typeChecker.checkOperandTypes(
            mockCallBinding(
                function,
                ImmutableList.of(new OperandSpec(SqlTypeName.INTEGER, false))
            ),
            true
        );
      });
      assertTrue(exception.getMessage().contains("Argument to function 'testLiteralOperandCheckLiteralThrow' must be a literal"));
    }

    @Test
    void anyTypeOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testAnyTypeOperand")
          .operandTypes(SqlTypeFamily.ANY)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertTrue(
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
    void castableFromDateTimestampToDatetimeFamily()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testCastableFromDatetimeFamilyToTimestamp")
          .operandTypes(SqlTypeFamily.DATETIME)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertTrue(
          typeChecker.checkOperandTypes(
              mockCallBinding(
                  function,
                  ImmutableList.of(new OperandSpec(SqlTypeName.DATE, false))
              ),
              true
          )
      );
      assertTrue(
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
    void nullForNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullForNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertTrue(
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
    void nullLiteralForNullableOperand()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testNullLiteralForNullableOperand")
          .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
      assertTrue(
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
    void nullForNullableOperandNonNullOutput()
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
      assertTrue(typeChecker.checkOperandTypes(binding, true));
      RelDataType returnType = function.getReturnTypeInference().inferReturnType(binding);
      assertFalse(returnType.isNullable());
    }

    @Test
    void nullForNullableOperandCascadeNullOutput()
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
      assertTrue(typeChecker.checkOperandTypes(binding, true));
      RelDataType returnType = function.getReturnTypeInference().inferReturnType(binding);
      assertTrue(returnType.isNullable());
    }

    @Test
    void nullForNullableOperandAlwaysNullableOutput()
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
      assertTrue(typeChecker.checkOperandTypes(binding, true));
      RelDataType returnType = function.getReturnTypeInference().inferReturnType(binding);
      assertTrue(returnType.isNullable());
    }

    @Test
    void nullForNonNullableOperand()
    {
      Throwable exception = assertThrows(CalciteContextException.class, () -> {
        SqlFunction function = OperatorConversions
            .operatorBuilder("testNullForNonNullableOperand")
            .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
            .requiredOperandCount(1)
            .returnTypeNonNull(SqlTypeName.CHAR)
            .build();
        SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
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
      });
      assertTrue(exception.getMessage().contains("Exception in test for operator[testNullForNonNullableOperand]: Illegal use of 'NULL'"));
    }

    @Test
    void nullLiteralForNonNullableOperand()
    {
      Throwable exception = assertThrows(CalciteContextException.class, () -> {
        SqlFunction function = OperatorConversions
            .operatorBuilder("testNullLiteralForNonNullableOperand")
            .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTERVAL_DAY_TIME)
            .requiredOperandCount(1)
            .returnTypeNonNull(SqlTypeName.CHAR)
            .build();
        SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
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
      });
      assertTrue(exception.getMessage().contains("Exception in test for operator[testNullLiteralForNonNullableOperand]: Illegal use of 'NULL'"));
    }

    @Test
    void nonCastableType()
    {
      Throwable exception = assertThrows(CalciteContextException.class, () -> {
        SqlFunction function = OperatorConversions
            .operatorBuilder("testNonCastableType")
            .operandTypes(SqlTypeFamily.CURSOR, SqlTypeFamily.INTERVAL_DAY_TIME)
            .requiredOperandCount(2)
            .returnTypeNonNull(SqlTypeName.CHAR)
            .build();
        SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();
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
      });
      assertTrue(exception.getMessage().contains("Exception in test for operator[testNonCastableType]: Cannot apply 'testNonCastableType' to arguments of type"));
    }

    @Test
    void signatureWithNames()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testSignatureWithNames")
          .operandNames("x", "y", "z")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE, SqlTypeFamily.ANY)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();

      assertEquals(
          "'testSignatureWithNames(<x>, [<y>, [<z>]])'",
          typeChecker.getAllowedSignatures(function, function.getName())
      );
    }

    @Test
    void signatureWithoutNames()
    {
      SqlFunction function = OperatorConversions
          .operatorBuilder("testSignatureWithoutNames")
          .operandTypes(SqlTypeFamily.INTEGER, SqlTypeFamily.DATE, SqlTypeFamily.ANY)
          .requiredOperandCount(1)
          .returnTypeNonNull(SqlTypeName.CHAR)
          .build();
      SqlOperandTypeChecker typeChecker = function.getOperandTypeChecker();

      assertEquals(
          "'testSignatureWithoutNames(<INTEGER>, [<DATE>, [<ANY>]])'",
          typeChecker.getAllowedSignatures(function, function.getName())
      );
    }

    private static SqlCallBinding mockCallBinding(
        SqlFunction function,
        List<OperandSpec> actualOperands
    )
    {
      SqlValidator validator = Mockito.mock(SqlValidator.class);
      Mockito.when(validator.getTypeFactory()).thenReturn(new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE));
      List<SqlNode> operands = new ArrayList<>(actualOperands.size());
      for (OperandSpec operand : actualOperands) {
        final SqlNode node;
        if (operand.isLiteral) {
          node = Mockito.mock(SqlLiteral.class);
          Mockito.when(node.getKind()).thenReturn(SqlKind.LITERAL);
        } else {
          node = Mockito.mock(SqlNode.class);
          // Setting this as SqlUtil.isLiteral makes a call on
          // node.getKind() and without this change would
          // return a NPE
          Mockito.when(node.getKind()).thenReturn(SqlKind.OTHER_FUNCTION);
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

    private static class OperandSpec
    {
      private final SqlTypeName type;
      private final boolean isLiteral;
      private final boolean isNullable;

      private OperandSpec(SqlTypeName type, boolean isLiteral)
      {
        this(type, isLiteral, type == SqlTypeName.NULL);
      }

      private OperandSpec(SqlTypeName type, boolean isLiteral, boolean isNullable)
      {
        this.type = type;
        this.isLiteral = isLiteral;
        this.isNullable = isNullable;
      }
    }
  }
}
