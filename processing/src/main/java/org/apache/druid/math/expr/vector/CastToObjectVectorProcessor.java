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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

public class CastToObjectVectorProcessor extends CastToTypeVectorProcessor<Object[]>
{
  private final ExpressionType outputType;
  private final ExpressionType delegateType;
  private final Object[] output;

  public CastToObjectVectorProcessor(
      ExprVectorProcessor<?> delegate,
      ExpressionType outputType,
      int maxVectorSize
  )
  {
    super(delegate);
    this.delegateType = delegate.getOutputType();
    this.outputType = outputType;
    this.output = new Object[maxVectorSize];
  }

  @Override
  public ExprEvalVector<Object[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<?> delegateOutput = delegate.evalVector(bindings);
    final Object[] toCast = delegateOutput.getObjectVector();
    for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
      ExprEval<?> cast = ExprEval.ofType(delegateType, toCast[i]).castTo(outputType);
      output[i] = cast.value();
    }
    return new ExprEvalObjectVector(output, outputType);
  }

  @Override
  public ExpressionType getOutputType()
  {
    return outputType;
  }
}
