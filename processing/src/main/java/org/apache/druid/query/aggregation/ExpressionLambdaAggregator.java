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

package org.apache.druid.query.aggregation;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.column.ObjectByteStrategy;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class ExpressionLambdaAggregator implements Aggregator
{
  private final Expr lambda;
  private final ExpressionLambdaAggregatorInputBindings bindings;
  private final int maxSizeBytes;
  private boolean hasValue;

  public ExpressionLambdaAggregator(
      final Expr lambda,
      final ExpressionLambdaAggregatorInputBindings bindings,
      final boolean isNullUnlessAggregated,
      final int maxSizeBytes
  )
  {
    this.lambda = lambda;
    this.bindings = bindings;
    this.maxSizeBytes = maxSizeBytes;
    this.hasValue = !isNullUnlessAggregated;
  }

  @Override
  public void aggregate()
  {
    final ExprEval<?> eval = lambda.eval(bindings);
    estimateAndCheckMaxBytes(eval, maxSizeBytes);
    bindings.accumulate(eval);
    hasValue = true;
  }

  @Nullable
  @Override
  public Object get()
  {
    return hasValue ? bindings.getAccumulator().value() : null;
  }

  @Override
  public float getFloat()
  {
    return (float) bindings.getAccumulator().asDouble();
  }

  @Override
  public long getLong()
  {
    return bindings.getAccumulator().asLong();
  }

  @Override
  public double getDouble()
  {
    return bindings.getAccumulator().asDouble();
  }

  @Override
  public boolean isNull()
  {
    return bindings.getAccumulator().isNumericNull();
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  /**
   * Tries to mimic the byte serialization of {@link Types} binary methods use to write expression values for the
   * {@link ExpressionLambdaBufferAggregator} in an attempt to provide consistent size limits when using the heap
   * based algorithm.
   */
  @VisibleForTesting
  public static void estimateAndCheckMaxBytes(ExprEval eval, int maxSizeBytes)
  {
    final int estimated;
    switch (eval.type().getType()) {
      case STRING:
        String stringValue = eval.asString();
        estimated = Integer.BYTES + (stringValue == null ? 0 : StringUtils.estimatedBinaryLengthAsUTF8(stringValue));
        break;
      case LONG:
      case DOUBLE:
        estimated = Long.BYTES;
        break;
      case ARRAY:
        switch (eval.type().getElementType().getType()) {
          case STRING:
            String[] stringArray = eval.asStringArray();
            if (stringArray == null) {
              estimated = Integer.BYTES;
            } else {
              final int elementsSize = Arrays.stream(stringArray)
                                             .filter(Objects::nonNull)
                                             .mapToInt(StringUtils::estimatedBinaryLengthAsUTF8)
                                             .sum();
              // since each value is variably sized, there is a null byte, and an integer length per element
              estimated = Integer.BYTES + (Integer.BYTES * stringArray.length) + elementsSize;
            }
            break;
          case LONG:
            Long[] longArray = eval.asLongArray();
            if (longArray == null) {
              estimated = Integer.BYTES;
            } else {
              final int elementsSize = Arrays.stream(longArray)
                                             .filter(Objects::nonNull)
                                             .mapToInt(x -> Long.BYTES)
                                             .sum();
              // null byte + length int + byte per element + size per element
              estimated = Integer.BYTES + longArray.length + elementsSize;
            }
            break;
          case DOUBLE:
            Double[] doubleArray = eval.asDoubleArray();
            if (doubleArray == null) {
              estimated = Integer.BYTES;
            } else {
              final int elementsSize = Arrays.stream(doubleArray)
                                             .filter(Objects::nonNull)
                                             .mapToInt(x -> Long.BYTES)
                                             .sum();
              // null byte + length int + byte per element + size per element
              estimated = Integer.BYTES + doubleArray.length + elementsSize;
            }
            break;
          default:
            throw new ISE("Unsupported array type: %s", eval.type());
        }
        break;
      case COMPLEX:
        final ObjectByteStrategy strategy = Types.getStrategy(eval.type().getComplexTypeName());
        if (strategy != null) {
          if (eval.value() != null) {
            // | null (byte) | length (int) | complex type bytes |
            final byte[] complexBytes = strategy.toBytes(eval.value());
            estimated = Integer.BYTES + complexBytes.length;
          } else {
            estimated = Integer.BYTES;
          }
        } else {
          throw new ISE("Unsupported type: %s", eval.type());
        }
        break;
      default:
        throw new ISE("Unsupported type: %s", eval.type());
    }
    // +1 for the null byte
    Types.checkMaxBytes(eval.type(), 1 + estimated, maxSizeBytes);
  }
}
