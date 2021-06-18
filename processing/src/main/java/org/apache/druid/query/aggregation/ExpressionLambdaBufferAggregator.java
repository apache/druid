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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class ExpressionLambdaBufferAggregator implements BufferAggregator
{
  private final Expr lambda;
  private final ExprEval<?> initialValue;
  private final ExpressionLambdaAggregatorInputBindings bindings;
  private final int maxSizeBytes;

  public ExpressionLambdaBufferAggregator(
      Expr lambda,
      ExprEval<?> initialValue,
      ExpressionLambdaAggregatorInputBindings bindings,
      int maxSizeBytes
  )
  {
    this.lambda = lambda;
    this.initialValue = initialValue;
    this.bindings = bindings;
    this.maxSizeBytes = maxSizeBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ExprEval.serialize(buf, position, initialValue, maxSizeBytes);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ExprEval<?> acc = ExprEval.deserialize(buf, position);
    bindings.setAccumulator(acc);
    ExprEval<?> newAcc = lambda.eval(bindings);
    ExprEval.serialize(buf, position, newAcc, maxSizeBytes);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return ExprEval.deserialize(buf, position).value();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) ExprEval.deserialize(buf, position).asDouble();
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return ExprEval.deserialize(buf, position).asDouble();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return ExprEval.deserialize(buf, position).asLong();
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
