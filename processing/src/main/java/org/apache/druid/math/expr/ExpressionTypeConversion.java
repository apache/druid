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

package org.apache.druid.math.expr;

import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class ExpressionTypeConversion
{
  /**
   * Infer the output type of a list of possible 'conditional' expression outputs (where any of these could be the
   * output expression if the corresponding case matching expression evaluates to true)
   */
  public static ExpressionType conditional(Expr.InputBindingInspector inspector, List<Expr> args)
  {
    ExpressionType type = null;
    for (Expr arg : args) {
      if (type == null) {
        type = arg.getOutputType(inspector);
      } else {
        type = function(type, arg.getOutputType(inspector));
      }
    }
    return type;
  }

  /**
   * Given 2 'input' types, which might not be fully trustable, choose the most appropriate combined type for
   * non-vectorized, per-row type detection. In this mode, null values are {@link ExprType#STRING} typed, despite
   * potentially coming from an underlying numeric column, or when an underlying column was completely missing and so
   * all values are null. This method is not well suited for array handling.
   */
  public static ExpressionType autoDetect(ExprEval eval, ExprEval otherEval)
  {
    ExpressionType type = eval.type();
    ExpressionType otherType = otherEval.type();
    if (type == otherType && type.getType().isPrimitive()) {
      return type;
    }
    if (Types.is(type, ExprType.STRING) && Types.is(otherType, ExprType.STRING)) {
      return ExpressionType.STRING;
    }
    // to preserve backwards compatibility, like with strings, we only use array type if both types are
    // arrays... this is pretty wack, but it is what it is. we might want to consider changing this
    // behavior in the future with a flag
    if (type.isArray() && otherType.isArray()) {
      return leastRestrictiveType(type, otherType);
    }

    type = eval.value() != null ? type : otherType;
    otherType = otherEval.value() != null ? otherType : type;
    return numeric(type, otherType);
  }

  /**
   * Given 2 'input' types, choose the most appropriate combined type, if possible
   *
   * arrays must be the same type
   * if both types are {@link ExprType#STRING}, the output type will be preserved as string
   * if both types are {@link ExprType#LONG}, the output type will be preserved as long
   * otherwise, output is {@link ExprType#DOUBLE}
   */
  @Nullable
  public static ExpressionType operator(@Nullable ExpressionType type, @Nullable ExpressionType other)
  {
    if (type == null) {
      return other;
    }
    if (other == null) {
      return type;
    }
    if (type.isArray() || other.isArray()) {
      if (!Objects.equals(type, other)) {
        throw new Types.IncompatibleTypeException(type, other);
      }
      return type;
    }
    if (type.is(ExprType.COMPLEX) || other.is(ExprType.COMPLEX)) {
      if (type.getComplexTypeName() == null) {
        return other;
      }
      if (other.getComplexTypeName() == null) {
        return type;
      }
      if (!Objects.equals(type, other)) {
        throw new Types.IncompatibleTypeException(type, other);
      }
      return type;
    }
    // if both arguments are a string, type becomes a string
    if (type.is(ExprType.STRING) && other.is(ExprType.STRING)) {
      return ExpressionType.STRING;
    }

    // otherwise a decimal or integer number
    return numeric(type, other);
  }

  /**
   * Given 2 'input' types, choose the most appropriate combined type, if possible
   *
   * arrays must be the same type
   * if either type is {@link ExprType#STRING}, the output type will be preserved as string
   * if both types are {@link ExprType#LONG}, the output type will be preserved as long, otherwise
   *  {@link ExprType#DOUBLE}
   */
  @Nullable
  public static ExpressionType function(@Nullable ExpressionType type, @Nullable ExpressionType other)
  {
    if (type == null) {
      return other;
    }
    if (other == null) {
      return type;
    }
    // arrays cannot be auto converted
    if (type.isArray() || other.isArray()) {
      if (!Objects.equals(type, other)) {
        throw new Types.IncompatibleTypeException(type, other);
      }
      return type;
    }
    if (type.is(ExprType.COMPLEX) || other.is(ExprType.COMPLEX)) {
      if (type.getComplexTypeName() == null) {
        return other;
      }
      if (other.getComplexTypeName() == null) {
        return type;
      }
      if (!Objects.equals(type, other)) {
        throw new Types.IncompatibleTypeException(type, other);
      }
      return type;
    }
    // if either argument is a string, type becomes a string
    if (Types.is(type, ExprType.STRING) || Types.is(other, ExprType.STRING)) {
      return ExpressionType.STRING;
    }

    return numeric(type, other);
  }

  /**
   * {@link ColumnType#leastRestrictiveType(ColumnType, ColumnType)} but for expression
   * types
   */
  @Nullable
  public static ExpressionType leastRestrictiveType(@Nullable ExpressionType type, @Nullable ExpressionType other)
      throws Types.IncompatibleTypeException
  {
    if (type == null) {
      return other;
    }
    if (other == null) {
      return type;
    }
    if (type.is(ExprType.COMPLEX) && other.is(ExprType.COMPLEX)) {
      if (type.getComplexTypeName() == null) {
        return other;
      }
      if (other.getComplexTypeName() == null) {
        return type;
      }
      if (!Objects.equals(type, other)) {
        throw new Types.IncompatibleTypeException(type, other);
      }
      return type;
    }
    // if either is nested data, use nested data, otherwise error
    if (type.is(ExprType.COMPLEX) || other.is(ExprType.COMPLEX)) {
      if (ExpressionType.NESTED_DATA.equals(type) || ExpressionType.NESTED_DATA.equals(other)) {
        return ExpressionType.NESTED_DATA;
      }
      throw new Types.IncompatibleTypeException(type, other);
    }

    // arrays convert based on least restrictive element type
    if (type.isArray()) {
      if (other.equals(type.getElementType())) {
        return type;
      }
      final ExpressionType commonElementType;
      // commonElementType cannot be null if we got this far, we always return a value unless both args are null
      if (other.isArray()) {
        commonElementType = leastRestrictiveType(
            (ExpressionType) type.getElementType(),
            (ExpressionType) other.getElementType()
        );

        return ExpressionTypeFactory.getInstance().ofArray(commonElementType);
      } else {
        commonElementType = leastRestrictiveType(
            (ExpressionType) type.getElementType(),
            other
        );
      }
      return ExpressionTypeFactory.getInstance().ofArray(commonElementType);
    }
    if (other.isArray()) {
      if (type.equals(type.getElementType())) {
        return type;
      }
      final ExpressionType commonElementType;

      commonElementType = leastRestrictiveType(
          type,
          (ExpressionType) other.getElementType()
      );
      return ExpressionTypeFactory.getInstance().ofArray(commonElementType);
    }
    // if either argument is a string, type becomes a string
    if (Types.is(type, ExprType.STRING) || Types.is(other, ExprType.STRING)) {
      return ExpressionType.STRING;
    }

    return numeric(type, other);
  }


  /**
   * Given 2 'input' types, choose the most appropriate combined type, if possible
   *
   * arrays must be the same type
   * if either type is {@link ExprType#STRING}, the output type will be preserved as string
   * any number will be coerced to {@link ExprType#LONG}
   */
  @Nullable
  public static ExpressionType integerMathFunction(@Nullable ExpressionType type, @Nullable ExpressionType other)
  {
    final ExpressionType functionType = ExpressionTypeConversion.function(type, other);
    // any number is long
    return Types.isNumeric(functionType) ? ExpressionType.LONG : functionType;
  }

  /**
   * Default best effort numeric type conversion. If both types are {@link ExprType#LONG}, returns
   * {@link ExprType#LONG}, else {@link ExprType#DOUBLE}
   */
  public static ExpressionType numeric(@Nullable ExpressionType type, @Nullable ExpressionType other)
  {
    // all numbers win over longs
    // floats vs doubles would be handled here, but we currently only support doubles...
    if (Types.is(type, ExprType.LONG) && Types.isNullOr(other, ExprType.LONG)) {
      return ExpressionType.LONG;
    }
    return ExpressionType.DOUBLE;
  }

  private ExpressionTypeConversion()
  {
    // no instantiation
  }
}
