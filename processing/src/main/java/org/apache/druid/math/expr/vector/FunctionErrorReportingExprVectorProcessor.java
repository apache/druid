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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionProcessingException;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.ExpressionValidationException;
import org.apache.druid.segment.column.Types;

/**
 * Wrapper around {@link ExprVectorProcessor} that reports errors in the same style as {@code FunctionExpr}.
 */
public class FunctionErrorReportingExprVectorProcessor<T> implements ExprVectorProcessor<T>
{
  private final String functionName;
  private final ExprVectorProcessor<T> delegate;

  public FunctionErrorReportingExprVectorProcessor(String functionName, ExprVectorProcessor<T> delegate)
  {
    this.functionName = functionName;
    this.delegate = delegate;
  }

  @Override
  public ExprEvalVector<T> evalVector(Expr.VectorInputBinding bindings)
  {
    try {
      return delegate.evalVector(bindings);
    }
    catch (ExpressionValidationException | ExpressionProcessingException e) {
      // Already contain function name
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(e, e.getMessage());
    }
    catch (Types.InvalidCastException | Types.InvalidCastBooleanException e) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(e, "Function[%s] encountered exception: %s", functionName, e.getMessage());
    }
    catch (DruidException | IAE e) {
      throw e;
    }
    catch (Exception e) {
      throw DruidException.defensive().build(e, "Function[%s] encountered unknown exception.", functionName);
    }
  }

  @Override
  public ExpressionType getOutputType()
  {
    return delegate.getOutputType();
  }

  @Override
  public int maxVectorSize()
  {
    return delegate.maxVectorSize();
  }
}
