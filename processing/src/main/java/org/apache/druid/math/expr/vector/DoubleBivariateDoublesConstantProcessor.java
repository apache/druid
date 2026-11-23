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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.simd.SimdSupportedBinaryOp;

import javax.annotation.Nullable;

/**
 * Double arithmetic processor with one operand bound to a constant.
 */
public final class DoubleBivariateDoublesConstantProcessor implements ExprVectorProcessor<double[]>
{
  private final ExprVectorProcessor<double[]> inputProcessor;
  private final double constant;
  private final boolean constantIsLeftOperand;
  private final SimdSupportedBinaryOp operation;
  private final boolean[] outNulls;
  private final double[] outValues;

  public DoubleBivariateDoublesConstantProcessor(
      ExprVectorProcessor<?> input,
      double constant,
      boolean constantIsLeftOperand,
      SimdSupportedBinaryOp operation
  )
  {
    this.inputProcessor = CastToTypeVectorProcessor.cast(input, ExpressionType.DOUBLE);
    this.constant = constant;
    this.constantIsLeftOperand = constantIsLeftOperand;
    this.operation = operation;
    this.outNulls = new boolean[input.maxVectorSize()];
    this.outValues = new double[input.maxVectorSize()];
  }

  @Override
  public ExprEvalVector<double[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<double[]> inputEval = inputProcessor.evalVector(bindings);
    final double[] input = inputEval.values();
    final boolean[] inputNulls = inputEval.getNullVector();
    final int currentSize = bindings.getCurrentVectorSize();
    final boolean anyNulls;

    // Select the operation and operand order once per vector. Keeping both decisions outside the hot loops lets
    // HotSpot compile direct arithmetic instead of a megamorphic DoubleBivariateDoublesFunction call for every row.
    switch (operation) {
      case ADD:
        anyNulls = processAdd(input, inputNulls, currentSize);
        break;
      case SUB:
        anyNulls = constantIsLeftOperand
                   ? processSubtractFromConstant(input, inputNulls, currentSize)
                   : processSubtractConstant(input, inputNulls, currentSize);
        break;
      case MUL:
        anyNulls = processMultiply(input, inputNulls, currentSize);
        break;
      case DIV:
        anyNulls = constantIsLeftOperand
                   ? processDivideConstantByInput(input, inputNulls, currentSize)
                   : processDivideInputByConstant(input, inputNulls, currentSize);
        break;
      default:
        throw new IllegalStateException("Unsupported constant arithmetic operation " + operation);
    }

    return new ExprEvalDoubleVector(outValues, anyNulls ? outNulls : null);
  }

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  public int maxVectorSize()
  {
    return outValues.length;
  }

  private boolean processAdd(double[] input, @Nullable boolean[] inputNulls, int currentSize)
  {
    if (inputNulls == null) {
      for (int i = 0; i < currentSize; i++) {
        outValues[i] = input[i] + constant;
      }
      return false;
    }
    boolean anyNulls = false;
    for (int i = 0; i < currentSize; i++) {
      final boolean isNull = inputNulls[i];
      outNulls[i] = isNull;
      if (isNull) {
        anyNulls = true;
        outValues[i] = 0.0;
      } else {
        outValues[i] = input[i] + constant;
      }
    }
    return anyNulls;
  }

  private boolean processSubtractFromConstant(double[] input, @Nullable boolean[] inputNulls, int currentSize)
  {
    if (inputNulls == null) {
      for (int i = 0; i < currentSize; i++) {
        outValues[i] = constant - input[i];
      }
      return false;
    }
    boolean anyNulls = false;
    for (int i = 0; i < currentSize; i++) {
      final boolean isNull = inputNulls[i];
      outNulls[i] = isNull;
      if (isNull) {
        anyNulls = true;
        outValues[i] = 0.0;
      } else {
        outValues[i] = constant - input[i];
      }
    }
    return anyNulls;
  }

  private boolean processSubtractConstant(double[] input, @Nullable boolean[] inputNulls, int currentSize)
  {
    if (inputNulls == null) {
      for (int i = 0; i < currentSize; i++) {
        outValues[i] = input[i] - constant;
      }
      return false;
    }
    boolean anyNulls = false;
    for (int i = 0; i < currentSize; i++) {
      final boolean isNull = inputNulls[i];
      outNulls[i] = isNull;
      if (isNull) {
        anyNulls = true;
        outValues[i] = 0.0;
      } else {
        outValues[i] = input[i] - constant;
      }
    }
    return anyNulls;
  }

  private boolean processMultiply(double[] input, @Nullable boolean[] inputNulls, int currentSize)
  {
    if (inputNulls == null) {
      for (int i = 0; i < currentSize; i++) {
        outValues[i] = input[i] * constant;
      }
      return false;
    }
    boolean anyNulls = false;
    for (int i = 0; i < currentSize; i++) {
      final boolean isNull = inputNulls[i];
      outNulls[i] = isNull;
      if (isNull) {
        anyNulls = true;
        outValues[i] = 0.0;
      } else {
        outValues[i] = input[i] * constant;
      }
    }
    return anyNulls;
  }

  private boolean processDivideConstantByInput(double[] input, @Nullable boolean[] inputNulls, int currentSize)
  {
    if (inputNulls == null) {
      for (int i = 0; i < currentSize; i++) {
        outValues[i] = constant / input[i];
      }
      return false;
    }
    boolean anyNulls = false;
    for (int i = 0; i < currentSize; i++) {
      final boolean isNull = inputNulls[i];
      outNulls[i] = isNull;
      if (isNull) {
        anyNulls = true;
        outValues[i] = 0.0;
      } else {
        outValues[i] = constant / input[i];
      }
    }
    return anyNulls;
  }

  private boolean processDivideInputByConstant(double[] input, @Nullable boolean[] inputNulls, int currentSize)
  {
    if (inputNulls == null) {
      for (int i = 0; i < currentSize; i++) {
        outValues[i] = input[i] / constant;
      }
      return false;
    }
    boolean anyNulls = false;
    for (int i = 0; i < currentSize; i++) {
      final boolean isNull = inputNulls[i];
      outNulls[i] = isNull;
      if (isNull) {
        anyNulls = true;
        outValues[i] = 0.0;
      } else {
        outValues[i] = input[i] / constant;
      }
    }
    return anyNulls;
  }
}
