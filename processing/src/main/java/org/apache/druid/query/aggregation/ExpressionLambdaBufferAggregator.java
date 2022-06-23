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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class ExpressionLambdaBufferAggregator implements BufferAggregator
{
  private static final short NOT_AGGREGATED_BIT = 1 << 7;
  private static final short IS_AGGREGATED_MASK = 0x3F;
  private final Expr lambda;
  private final ExprEval<?> initialValue;
  private final ExpressionLambdaAggregatorInputBindings bindings;
  private final int maxSizeBytes;
  private final boolean isNullUnlessAggregated;
  private final boolean aggregateNullInputs;
  private final List<String> inputColumns;
  private final ExpressionType outputType;

  public ExpressionLambdaBufferAggregator(
      final ExpressionLambdaAggregatorFactory.FactorizePlan thePlan,
      int maxSizeBytes
  )
  {
    this.lambda = thePlan.getExpression();
    this.initialValue = thePlan.getInitialValue();
    this.outputType = initialValue.type();
    this.bindings = thePlan.getBindings();
    this.isNullUnlessAggregated = thePlan.isNullUnlessAggregated();
    this.aggregateNullInputs = thePlan.shouldAggregateNullInputs();
    this.inputColumns = thePlan.getInputs();
    this.maxSizeBytes = maxSizeBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ExprEval.serialize(buf, position, outputType, initialValue, maxSizeBytes);
    // set a bit to indicate we haven't aggregated on top of expression type (not going to lie this could be nicer)
    if (isNullUnlessAggregated) {
      buf.put(position, (byte) (buf.get(position) | NOT_AGGREGATED_BIT));
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (!aggregateNullInputs) {
      for (String column : inputColumns) {
        if (bindings.get(column) == null) {
          return;
        }
      }
    }
    ExprEval<?> acc = ExprEval.deserialize(buf, position, maxSizeBytes, outputType, true);
    bindings.setAccumulator(acc);
    ExprEval<?> newAcc = lambda.eval(bindings);
    ExprEval.serialize(buf, position, outputType, newAcc, maxSizeBytes);
    // scrub not aggregated bit
    buf.put(position, (byte) (buf.get(position) & IS_AGGREGATED_MASK));
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    if (isNullUnlessAggregated && (buf.get(position) & NOT_AGGREGATED_BIT) != 0) {
      return null;
    }
    return ExprEval.deserialize(buf, position, maxSizeBytes, outputType, false).value();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) ExprEval.deserialize(buf, position, maxSizeBytes, outputType, true).asDouble();
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return ExprEval.deserialize(buf, position, maxSizeBytes, outputType, true).asDouble();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return ExprEval.deserialize(buf, position, maxSizeBytes, outputType, true).asLong();
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
