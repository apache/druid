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

import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.util.NlsString;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.Collections;
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
          o = callBinding.getOperandLiteralValue(i, Object.class);
        } else {
          throw new NonLiteralException();
        }

        final Object o2 = coerce(o, type);
        arguments.add(o2);
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

  // Copy of Calcite method with Druid-specific code added
  private static Object coerce(Object o, RelDataType type) throws NonLiteralException
  {
    if (o == null) {
      return null;
    }
    // Druid-specific code to handle arrays. Although the type
    // is called an ARRAY in SQL, the actual argument is a generic
    // list, which we then convert to another list with the elements
    // coerced to the declared element type.
    if (type instanceof ArraySqlType) {
      RelDataType elementType = ((ArraySqlType) type).getComponentType();
      if (!(elementType instanceof RelDataTypeFactoryImpl.JavaType)) {
        throw new NonLiteralException();
      }

      // If a list (ARRAY), then coerce each member.
      if (!(o instanceof List)) {
        throw new NonLiteralException();
      }
      List<?> arg = (List<?>) o;
      List<Object> revised = new ArrayList<>(arg.size());
      for (Object value : arg) {
        Object element = coerce(value, elementType);
        if (element == null) {
          throw new NonLiteralException();
        }
        revised.add(element);
      }
      return revised;
    }
    if (!(type instanceof RelDataTypeFactoryImpl.JavaType)) {
      // If the type can't be converted, raise an error. Calcite returns null
      // which causes odd downstream failures that are hard to diagnose.
      throw new NonLiteralException();
    }
    final RelDataTypeFactoryImpl.JavaType javaType =
        (RelDataTypeFactoryImpl.JavaType) type;
    final Class<?> clazz = javaType.getJavaClass();
    //noinspection unchecked
    if (clazz.isAssignableFrom(o.getClass())) {
      return o;
    }
    if (clazz == String.class && o instanceof NlsString) {
      return ((NlsString) o).getValue();
    }
    // We need optimization here for constant folding.
    // Not all the expressions can be interpreted (e.g. ternary), so
    // we rely on optimization capabilities to fold non-interpretable
    // expressions.
    BlockBuilder bb = new BlockBuilder();
    final Expression expr =
        EnumUtils.convert(Expressions.constant(o), clazz);
    bb.add(Expressions.return_(null, expr));
    final FunctionExpression<?> convert =
        Expressions.lambda(bb.toBlock(), Collections.emptyList());
    return convert.compile().dynamicInvoke();
  }

  /**
   * Thrown when a non-literal occurs in an argument to a user-defined
   * table macro.
   */
  private static class NonLiteralException extends Exception
  {
  }
}
