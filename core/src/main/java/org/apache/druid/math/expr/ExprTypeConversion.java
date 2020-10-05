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
  static ExprType conditional(Expr.InputBindingTypes inputTypes, List<Expr> args)
  {
    ExprType type = null;
    for (Expr arg : args) {
      if (arg.isNullLiteral()) {
        continue;
      }
      if (type == null) {
        type = arg.getOutputType(inputTypes);
      } else {
        type = doubleMathFunction(type, arg.getOutputType(inputTypes));
      }
    }
    return type;
  }

  /**
   * Given 2 'input' types, choose the most appropriate combined type, if possible
   *
   * arrays must be the same type
   * if both types are {@link ExprType#STRING}, the output type will be preserved as string
   * if both types are {@link ExprType#LONG}, the output type will be preserved as long
   *
   */
  @Nullable
  public static ExprType operator(@Nullable ExprType type, @Nullable ExprType other)
  {
    if (type == null || other == null) {
      // cannot auto conversion unknown types
      return null;
    }
    // arrays cannot be auto converted
    if (ExprType.isArray(type) || ExprType.isArray(other)) {
      if (!type.equals(other)) {
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
  public static ExprType doubleMathFunction(@Nullable ExprType type, @Nullable ExprType other)
  {
    if (type == null || other == null) {
      // cannot auto conversion unknown types
      return null;
    }
    // arrays cannot be auto converted
    if (ExprType.isArray(type) || ExprType.isArray(other)) {
      if (!type.equals(other)) {
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
    if (type == null || other == null) {
      // cannot auto conversion unknown types
      return null;
    }
    // arrays cannot be auto converted
    if (ExprType.isArray(type) || ExprType.isArray(other)) {
      if (!type.equals(other)) {
        throw new IAE("Cannot implicitly cast %s to %s", type, other);
      }
      return type;
    }
    // if either argument is a string, type becomes a string
    if (ExprType.STRING.equals(type) || ExprType.STRING.equals(other)) {
      return ExprType.STRING;
    }

    // any number is long
    return ExprType.LONG;
  }

  /**
   * Default best effort numeric type conversion. If both types are {@link ExprType#LONG}, returns
   * {@link ExprType#LONG}, else {@link ExprType#DOUBLE}
   */
  public static ExprType numeric(ExprType type, ExprType other)
  {
    // all numbers win over longs
    if (ExprType.LONG.equals(type) && ExprType.LONG.equals(other)) {
      return ExprType.LONG;
    }
    // floats vs doubles would be handled here, but we currently only support doubles...
    return ExprType.DOUBLE;
  }

  private ExprTypeConversion()
  {
    // no instantiation
  }
}
