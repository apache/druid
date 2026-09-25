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

public final class CastToLongVectorProcessor extends CastToTypeVectorProcessor<long[]>
{
  // Processor-owned scratch space reused for each double-to-long conversion. Results are valid until the next
  // evaluation of this processor, consistent with the lifetime of output arrays from other vector processors.
  private final long[] output;

  public CastToLongVectorProcessor(ExprVectorProcessor<?> delegate)
  {
    super(delegate);
    this.output = new long[delegate.maxVectorSize()];
  }

  @Override
  public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<?> result = delegate.evalVector(bindings);
    final long[] values;
    if (delegate.getOutputType().equals(ExpressionType.DOUBLE)) {
      final double[] input = result.getDoubleVector();
      for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
        output[i] = (long) input[i];
      }
      values = output;
    } else {
      values = result.getLongVector();
    }
    return new ExprEvalLongVector(values, result.getNullVector());
  }

  @Override
  public ExpressionType getOutputType()
  {
    return ExpressionType.LONG;
  }
}
