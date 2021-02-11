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

import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.List;

public class ExprTypeConversion
{
  /**
   * Infer the output type of a list of possible 'conditional' expression outputs (where any of these could be the
   * output expression if the corresponding case matching expression evaluates to true)
   */
  public static ExprType conditional(Expr.InputBindingInspector inspector, List<Expr> args)
  {
    ExprType type = null;
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
  public static ExprType autoDetect(ExprEval eval, ExprEval otherEval)
  {
    ExprType type = eval.type();
    ExprType otherType = otherEval.type();
    if (type == ExprType.STRING && otherType == ExprType.STRING) {
      return ExprType.STRING;
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
  public static ExprType operator(@Nullable ExprType type, @Nullable ExprType other)
  {
    if (type == null) {
      return other;
    }
    if (other == null) {
      return type;
    }
    if (ExprType.isArray(type) || ExprType.isArray(other)) {
      if (type != other) {
        throw new IAE("Cannot implicitly cast %s to %s", type, other);
      }
      return type;
    }
    // if both arguments are a string, type becomes a string
    if (ExprType.STRING.equals(type) && ExprType.STRING.equals(other)) {
      return ExprType.STRING;
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
  public static ExprType function(@Nullable ExprType type, @Nullable ExprType other)
  {
    if (type == null) {
      type = other;
    }
    if (other == null) {
      other = type;
    }
    // arrays cannot be auto converted
    if (ExprType.isArray(type) || ExprType.isArray(other)) {
      if (type != other) {
        throw new IAE("Cannot implicitly cast %s to %s", type, other);
      }
      return type;
    }
    // if either argument is a string, type becomes a string
    if (ExprType.STRING.equals(type) || ExprType.STRING.equals(other)) {
      return ExprType.STRING;
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
  public static ExprType integerMathFunction(@Nullable ExprType type, @Nullable ExprType other)
  {
    final ExprType functionType = ExprTypeConversion.function(type, other);
    // any number is long
    return ExprType.isNumeric(functionType) ? ExprType.LONG : functionType;
  }

  /**
   * Default best effort numeric type conversion. If both types are {@link ExprType#LONG}, returns
   * {@link ExprType#LONG}, else {@link ExprType#DOUBLE}
   */
  public static ExprType numeric(@Nullable ExprType type, @Nullable ExprType other)
  {
    // all numbers win over longs
    // floats vs doubles would be handled here, but we currently only support doubles...
    if (ExprType.LONG.equals(type) && ExprType.LONG.equals(other)) {
      return ExprType.LONG;
    }
    return ExprType.DOUBLE;
  }

  private ExprTypeConversion()
  {
    // no instantiation
  }
}
