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
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.ExpressionTypeConversion;

public class VectorConditionalProcessors
{
  public static <T> ExprVectorProcessor<T> nvl(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    final ExpressionType outputType = ExpressionTypeConversion.leastRestrictiveType(leftType, rightType);

    final ExprVectorProcessor<?> processor;
    if (outputType.is(ExprType.LONG)) {
      // long is most restrictive so both processors are definitely long typed if output is long
      processor = new NvlLongVectorProcessor(
          left.asVectorProcessor(inspector),
          right.asVectorProcessor(inspector)
      );
    } else if (outputType.is(ExprType.DOUBLE)) {
      processor = new NvlDoubleVectorProcessor(
          CastToTypeVectorProcessor.cast(left.asVectorProcessor(inspector), ExpressionType.DOUBLE),
          CastToTypeVectorProcessor.cast(right.asVectorProcessor(inspector), ExpressionType.DOUBLE)
      );
    } else {
      processor = new NvlVectorObjectProcessor(
          outputType,
          CastToTypeVectorProcessor.cast(left.asVectorProcessor(inspector), outputType),
          CastToTypeVectorProcessor.cast(right.asVectorProcessor(inspector), outputType)
      );
    }
    return (ExprVectorProcessor<T>) processor;
  }
}
