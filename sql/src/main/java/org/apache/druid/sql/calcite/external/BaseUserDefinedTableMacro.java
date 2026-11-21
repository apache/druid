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

package org.apache.druid.sql.calcite.external;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.NlsString;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.planner.DruidSqlValidator;
import org.apache.druid.sql.calcite.planner.SqlParameterizerShuttle;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Druid-specific version of {@link SqlUserDefinedTableMacro} which
 * copies & overrides a bunch of code to handle string array arguments.
 * Would be best if Calcite handled such argument: retire this class if
 * we upgrade to a version of Calcite that handles this task.
 */
public class BaseUserDefinedTableMacro extends SqlUserDefinedTableMacro
{
  protected final TableMacro macro;

  public BaseUserDefinedTableMacro(
      final SqlIdentifier opName,
      final SqlReturnTypeInference returnTypeInference,
      final SqlOperandTypeInference operandTypeInference,
      final SqlOperandMetadata operandMetadata,
      final TableMacro tableMacro
  )
  {
    super(opName, SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeInference, operandMetadata, tableMacro);

    // Because Calcite's copy of the macro is private
    this.macro = tableMacro;
  }

  /**
   * Copy of Calcite method {@link SqlUserDefinedTableMacro#getTable} to add array and named parameter handling.
   */
  @Override
  public TranslatableTable getTable(SqlOperatorBinding callBinding)
  {
    List<Object> arguments = convertArguments(callBinding, macro, getNameAsId(), true);
    return macro.apply(arguments);
  }

  /**
   * Similar to Calcite method {@link SqlUserDefinedTableMacro#convertArguments}, but with array and
   * named parameter handling.
   */
  public static List<Object> convertArguments(
      SqlOperatorBinding callBinding,
      Function function,
      SqlIdentifier opName,
      boolean failOnNonLiteral
  )
  {
    final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
    final List<FunctionParameter> parameters = function.getParameters();
    final List<Object> arguments = new ArrayList<>(callBinding.getOperandCount());

    for (int i = 0; i < parameters.size(); i++) {
      final FunctionParameter parameter = parameters.get(i);
      final RelDataType type = parameter.getType(typeFactory);
      try {
        final Object o;

        if (callBinding.isOperandLiteral(i, true)) {
          if (containsExplicitCast(callBinding, i)) {
            throw InvalidSqlInput.exception(
                "Argument [%s] of table function [%s] must be a literal, not an expression. Remove the CAST.",
                parameter.getName(),
                opName
            );
          }
          o = callBinding.getOperandLiteralValue(i, Object.class);
        } else {
          throw new NonLiteralException();
        }

        if (o != null && type.getSqlTypeName() == SqlTypeName.BIGINT && isApproximateNumeric(callBinding, i)) {
          throw new IncompatibleLiteralException();
        }
        arguments.add(convertLiteral(o, type));
      }
      catch (IncompatibleLiteralException e) {
        throw e.toInvalidSqlInput(callBinding, i, parameter.getName(), opName, type);
      }
      catch (NonLiteralException e) {
        if (failOnNonLiteral) {
          throw new IAE(
              "All arguments of call to macro %s should be literal. Actual argument #%d (%s) is not literal",
              opName,
              parameter.getOrdinal(),
              parameter.getName()
          );
        }

        final Object value;
        if (type.isNullable()) {
          value = null;
        } else {
          // Odd default, given that we don't know the type is numeric. But this is what Calcite does upstream
          // in SqlUserDefinedTableMacro.
          value = 0L;
        }
        arguments.add(value);
      }
    }

    return arguments;
  }

  /**
   * Returns whether operand {@code i} contains an explicit CAST of a literal, as opposed to one added by type
   * coercion. {@link SqlOperatorBinding#isOperandLiteral} accepts such a CAST, but it adds nothing to a literal.
   * A CAST of a bound dynamic parameter is allowed, since
   * {@link SqlParameterizerShuttle} has already replaced the parameter with a literal before validation.
   */
  private static boolean containsExplicitCast(SqlOperatorBinding callBinding, int i)
  {
    if (!(callBinding instanceof SqlCallBinding)) {
      return false;
    }
    final SqlCallBinding sqlCallBinding = (SqlCallBinding) callBinding;
    return containsExplicitCast(sqlCallBinding.operand(i), sqlCallBinding.getValidator());
  }

  private static boolean containsExplicitCast(SqlNode node, SqlValidator validator)
  {
    switch (node.getKind()) {
      case CAST:
        final SqlNode operand = ((SqlCall) node).operand(0);
        if (node.getParserPosition().equals(operand.getParserPosition())
            || node.getParserPosition().getLineNum() == 0) {
          // Implicit CAST added by type coercion, which gives the CAST the position of its operand.
          // Coercion of ARRAY and MAP elements starts the position at line 0, which the parser never assigns.
          return containsExplicitCast(operand, validator);
        }
        return !(validator instanceof DruidSqlValidator
                 && ((DruidSqlValidator) validator).getPlannerContext().isBoundParameter(operand));
      case ARRAY_VALUE_CONSTRUCTOR:
      case MAP_VALUE_CONSTRUCTOR:
        return ((SqlCall) node).getOperandList().stream().anyMatch(o -> containsExplicitCast(o, validator));
      default:
        return false;
    }
  }

  /**
   * Returns whether operand {@code index} is an approximate numeric literal.
   * "Approximate numeric" is the SQL term for floating-point values, as opposed to exact INTEGER and DECIMAL values.
   * It depends on how the literal is written, not on whether the value has a fraction.
   * <ul>
   * <li>{@code 100} is an exact INTEGER: false.</li>
   * <li>{@code 100.0} and {@code 1.5} are exact DECIMALs: false.</li>
   * <li>{@code 1e2} and {@code 1.5E0} are written with an exponent, which makes them DOUBLEs: true.</li>
   * <li>A dynamic parameter bound as BIGINT becomes an exact literal: false.</li>
   * </ul>
   * An approximate value is rejected for a BIGINT parameter even when it is integral, such as {@code 1e2}.
   * An exact value with a fraction, such as {@code 1.5}, is not handled here, {@link #convertLiteral} rejects it.
   */
  private static boolean isApproximateNumeric(final SqlOperatorBinding binding, final int index)
  {
    if (binding instanceof SqlCallBinding sqlBinding) {
      // Use the original literal's type.
      // Calcite may already have added a CAST to the declared parameter type.
      return stripCasts(sqlBinding.operand(index)) instanceof SqlNumericLiteral literal && !literal.isExact();
    }
    return SqlTypeName.APPROX_TYPES.contains(binding.getOperandType(index).getSqlTypeName());
  }

  private static SqlNode stripCasts(final SqlNode node)
  {
    SqlNode stripped = node;
    while (stripped.getKind() == SqlKind.CAST) {
      stripped = ((SqlCall) stripped).operand(0);
    }
    return stripped;
  }

  /**
   * Converts Calcite's literal values to the Java representation of a compatible parameter.
   * Returns null for SQL NULL.
   * <p>
   * Handles the parameter types that table functions can declare: VARCHAR, BIGINT, BOOLEAN and VARCHAR ARRAY.
   * A new parameter type needs its own conversion here.
   *
   * @throws IncompatibleLiteralException if the value is incompatible with the type
   */
  @Nullable
  private static Object convertLiteral(@Nullable final Object value, final RelDataType type)
      throws IncompatibleLiteralException
  {
    if (value == null) {
      return null;
    }

    if (type instanceof ArraySqlType && value instanceof List<?> list) {
      final RelDataType elementType = type.getComponentType();
      final List<Object> converted = new ArrayList<>(list.size());
      for (int i = 0; i < list.size(); i++) {
        final Object element = list.get(i);
        if (element == null) {
          throw new IncompatibleLiteralException(i, false);
        }
        try {
          converted.add(convertLiteral(element, elementType));
        }
        catch (IncompatibleLiteralException e) {
          throw new IncompatibleLiteralException(i, e.outOfRange);
        }
      }
      return converted;
    }

    if (type instanceof RelDataTypeFactoryImpl.JavaType javaType) {
      final Class<?> targetClass = javaType.getJavaClass();
      return switch (value) {
        case NlsString literal when targetClass == String.class -> literal.getValue();
        case BigDecimal numeric when targetClass == Long.class -> {
          try {
            yield numeric.longValueExact();
          }
          catch (ArithmeticException e) {
            // A value with a fraction is reported by its type instead.
            throw new IncompatibleLiteralException(null, numeric.stripTrailingZeros().scale() <= 0);
          }
        }
        // Calcite uses NlsString for character literals.
        // A plain String can come from DATE or TIME syntax.
        case Object literal when targetClass != String.class && targetClass.isInstance(literal) -> literal;
        default -> throw new IncompatibleLiteralException();
      };
    }
    throw new IncompatibleLiteralException();
  }

  /**
   * Thrown when a literal argument is incompatible with the declared type of its parameter.
   * Builds the error reported to the user, see {@link #toInvalidSqlInput}.
   */
  private static class IncompatibleLiteralException extends Exception
  {
    /**
     * Zero-based index of the offending array element, or null if the argument itself is incompatible.
     */
    @Nullable
    private final Integer elementIndex;

    /**
     * Whether the literal is an integer that does not fit the parameter type.
     */
    private final boolean outOfRange;

    private IncompatibleLiteralException()
    {
      this(null, false);
    }

    private IncompatibleLiteralException(@Nullable final Integer elementIndex, final boolean outOfRange)
    {
      this.elementIndex = elementIndex;
      this.outOfRange = outOfRange;
    }

    /**
     * Converts this exception to the error reported to the user.
     *
     * @param binding       the table function call
     * @param index         position of the incompatible operand in the call
     * @param parameterName name of the parameter that the operand is bound to
     * @param opName        name of the table function
     * @param expectedType  declared type of the parameter
     */
    private DruidException toInvalidSqlInput(
        final SqlOperatorBinding binding,
        final int index,
        final String parameterName,
        final SqlIdentifier opName,
        final RelDataType expectedType
    )
    {
      final RelDataType componentType = expectedType.getComponentType();
      final String expectedTypeName = componentType == null
                                      ? expectedType.getSqlTypeName().getName()
                                      : componentType.getSqlTypeName().getName() + " ARRAY";
      return InvalidSqlInput.exception(
          this,
          "Argument [%s] of table function [%s] must be a literal of type [%s] or a compatible type%s.",
          parameterName,
          opName,
          expectedTypeName,
          describeActual(binding, index)
      );
    }

    /**
     * Describes what the user passed for operand {@code index}, as a suffix for the error message.
     * Names the type of the offending literal rather than its value, since arguments such as passwords are secrets.
     * Returns an empty string when the literal cannot be described.
     */
    private String describeActual(final SqlOperatorBinding binding, final int index)
    {
      final String subject = elementIndex == null ? "" : StringUtils.format("element [%d] ", elementIndex + 1);
      if (outOfRange) {
        return StringUtils.format(", but %sis out of range", subject);
      }
      if (!(binding instanceof SqlCallBinding sqlBinding)) {
        return "";
      }

      SqlNode node = stripCasts(sqlBinding.operand(index));
      if (elementIndex != null) {
        if (node.getKind() != SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
          return "";
        }
        node = stripCasts(((SqlCall) node).operand(elementIndex));
      }

      if (SqlUtil.isNullLiteral(node, false)) {
        return StringUtils.format(", but %sis NULL", subject);
      }
      final String typeName = literalTypeName(node);
      return typeName == null ? "" : StringUtils.format(", but %sis of type [%s]", subject, typeName);
    }

    /**
     * Returns the SQL type name of a literal as the user wrote it, or null if the node is not a recognized literal.
     */
    @Nullable
    private String literalTypeName(final SqlNode node)
    {
      return switch (node) {
        // DATE and TIME literals stay unresolved until validation assigns them a type.
        case SqlUnknownLiteral unknown -> unknown.tag;
        case SqlNumericLiteral numeric when !numeric.isExact() -> SqlTypeName.DOUBLE.getName();
        // Calcite types every exact numeric literal as DECIMAL.
        // Report a literal without a fraction as INTEGER, which is how the user wrote it.
        case SqlNumericLiteral numeric when numeric.isInteger() -> SqlTypeName.INTEGER.getName();
        case SqlLiteral literal when literal.getTypeName() == SqlTypeName.CHAR -> SqlTypeName.VARCHAR.getName();
        case SqlLiteral literal -> literal.getTypeName().getName();
        default -> switch (node.getKind()) {
          case ARRAY_VALUE_CONSTRUCTOR -> SqlTypeName.ARRAY.getName();
          case MAP_VALUE_CONSTRUCTOR -> SqlTypeName.MAP.getName();
          default -> null;
        };
      };
    }
  }

  /**
   * Thrown when a non-literal occurs in an argument to a user-defined
   * table macro.
   */
  private static class NonLiteralException extends Exception
  {
  }
}
