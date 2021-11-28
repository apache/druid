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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;

import javax.annotation.Nullable;

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
    final int estimatedSize = eval.type().getNullableStrategy().estimateSizeBytes(eval.value());
    if (estimatedSize > maxSizeBytes) {
      throw new ISE(
          "Exceeded memory usage when aggregating type [%s], size [%s] is larger than max [%s]",
          eval.type().asTypeString(),
          estimatedSize,
          maxSizeBytes
      );
    }
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
}
