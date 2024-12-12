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

import net.thisptr.jackson.jq.internal.misc.Strings;

import java.util.Arrays;
import java.util.List;

/**
 * Common stuff for "named" functions of "functional" expressions, such as {@link FunctionExpr},
 * {@link ApplyFunctionExpr}, and {@link ExprMacroTable.ExprMacroFunctionExpr}.
 *
 * Provides helper methods for performing common validation operations to help reduce boilerplate for implementors and
 * make it easier to provide consistent error messaging.
 */
public interface NamedFunction
{
  /**
   * Name of the function
   */
  String name();

  /**
   * Helper method for creating a {@link ExpressionValidationException} with the specified reason
   */
  default ExpressionValidationException validationFailed(String reasonFormat, Object... args)
  {
    throw new ExpressionValidationException(this, reasonFormat, args);
  }

  default ExpressionValidationException validationFailed(Throwable e, String reasonFormat, Object... args)
  {
    throw new ExpressionValidationException(this, e, reasonFormat, args);
  }

  default ExpressionProcessingException processingFailed(Throwable e, String reasonFormat, Object... args)
  {
    throw new ExpressionProcessingException(this, e, reasonFormat, args);
  }

  /**
   * Helper method for implementors performing validation to check if the argument count is expected
   */
  default void validationHelperCheckArgumentCount(List<Expr> args, int count)
  {
    if (args.size() != count) {
      if (count == 0) {
        throw validationFailed("does not accept arguments");
      } else if (count == 1) {
        throw validationFailed("requires 1 argument");
      }
      throw validationFailed("requires %d arguments", count);
    }
  }

  /**
   * Helper method for implementors performing validation to check if there are at least as many arguments as specified
   */
  default void validationHelperCheckMinArgumentCount(List<Expr> args, int count)
  {
    if (args.size() < count) {
      if (count == 1) {
        throw validationFailed("requires at least 1 argument");
      }
      throw validationFailed("requires at least %d arguments", count);
    }
  }

  default void validationHelperCheckArgumentRange(List<Expr> args, int start, int end)
  {
    if (args.size() < start || args.size() > end) {
      throw validationFailed("requires %d to %d arguments", start, end);
    }
  }

  /**
   * Helper method for implementors performing validation to check if argument count is any of specified counts
   */
  default void validationHelperCheckAnyOfArgumentCount(List<Expr> args, int... counts)
  {
    boolean satisfied = false;
    for (int count : counts) {
      if (args.size() == count) {
        satisfied = true;
        break;
      }
    }
    if (!satisfied) {
      throw validationFailed(
          "requires %s arguments",
          Strings.join(" or ", () -> Arrays.stream(counts).mapToObj(String::valueOf).iterator())
      );
    }
  }

  /**
   * Helper method for implementors performing validation to check that an argument is a literal
   */
  default void validationHelperCheckArgIsLiteral(Expr arg, String argName)
  {
    if (!arg.isLiteral()) {
      throw validationFailed(
          "%s argument must be a literal",
          argName
      );
    }
  }

  /**
   * Helper method for implementors performing validation to check that the argument list is some expected size.
   *
   * The parser decomposes a function like 'fold((x, acc) -> x + acc, col, 0)' into the {@link LambdaExpr}
   * '(x, acc) -> x + acc' and the list of arguments, ['col', 0], and so does not include the {@link LambdaExpr} here.
   * To compensate for this, the error message will indicate that at least count + 1 arguments are required to count
   * the lambda.
   */
  default void validationHelperCheckArgumentCount(LambdaExpr lambdaExpr, List<Expr> args, int count)
  {
    if (args.size() != count) {
      throw validationFailed("requires %s arguments", count + 1);
    }
    validationHelperCheckLambaArgumentCount(lambdaExpr, args);
  }

  /**
   * Helper method for implementors performing validation to check that the argument list is at least some
   * expected size.
   *
   * The parser decomposes a function like 'fold((x, acc) -> x + acc, col, 0)' into the {@link LambdaExpr}
   * '(x, acc) -> x + acc' and the list of arguments, ['col', 0], and so does not include the {@link LambdaExpr} here.
   * To compensate for this, the error message will indicate that at least count + 1 arguments are required to count
   * the lambda.
   */
  default void validationHelperCheckMinArgumentCount(LambdaExpr lambdaExpr, List<Expr> args, int count)
  {
    if (args.size() < count) {
      throw validationFailed("requires at least %d arguments", count + 1);
    }
    validationHelperCheckLambaArgumentCount(lambdaExpr, args);
  }

  /**
   * Helper method for implementors performing validation to check that the {@link LambdaExpr#identifierCount()}
   * matches the number of arguments being passed to it
   */
  default void validationHelperCheckLambaArgumentCount(LambdaExpr lambdaExpr, List<Expr> args)
  {
    if (args.size() != lambdaExpr.identifierCount()) {
      throw validationFailed(
          "lambda expression argument count of %d does not match the %d arguments passed to it",
          lambdaExpr.identifierCount(),
          args.size()
      );
    }
  }
}
