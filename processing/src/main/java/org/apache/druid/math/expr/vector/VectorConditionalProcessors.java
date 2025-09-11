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

import java.util.List;

public class VectorConditionalProcessors
{
  public static <T> ExprVectorProcessor<T> nvl(Expr.VectorInputBindingInspector inspector, Expr left, Expr right)
  {
    final ExpressionType leftType = left.getOutputType(inspector);
    final ExpressionType rightType = right.getOutputType(inspector);
    final ExpressionType outputType = ExpressionTypeConversion.leastRestrictiveType(leftType, rightType);

    final ExprVectorProcessor<?> processor;
    if (outputType == null) {
      // if output type is null, it means all the input types were null (non-existent), and nvl(null, null) is null
      return VectorProcessors.constant((Long) null, inspector.getMaxVectorSize());
    }
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

  public static <T> ExprVectorProcessor<T> ifFunction(
      Expr.VectorInputBindingInspector inspector,
      Expr conditionExpr,
      Expr thenExpr,
      Expr elseExpr
  )
  {
    // right now canVectorize verifies that all args have the same output type, but this is aspirational towards what
    // the logic probably should be and is using least restrictive type. if the canVectorize logic changes to something
    // other than least restrictive type, then so should this
    final ExpressionType outputType = ExpressionTypeConversion.leastRestrictiveType(
        thenExpr.getOutputType(inspector),
        elseExpr.getOutputType(inspector)
    );

    final ExprVectorProcessor<?> processor;
    if (outputType == null) {
      // if output type is null, it means all the input types were null (non-existent), and if(null, null, null) is null
      return VectorProcessors.constant((Long) null, inspector.getMaxVectorSize());
    }
    if (outputType.is(ExprType.LONG)) {
      // long is most restrictive so both processors are definitely long typed if output is long
      processor = new IfLongVectorProcessor(
          conditionExpr.asVectorProcessor(inspector),
          thenExpr.asVectorProcessor(inspector),
          elseExpr.asVectorProcessor(inspector)
      );
    } else if (outputType.is(ExprType.DOUBLE)) {
      processor = new IfDoubleVectorProcessor(
          conditionExpr.asVectorProcessor(inspector),
          thenExpr.asVectorProcessor(inspector),
          elseExpr.asVectorProcessor(inspector)
      );
    } else {
      processor = new IfObjectVectorProcessor(
          outputType,
          conditionExpr.asVectorProcessor(inspector),
          thenExpr.asVectorProcessor(inspector),
          elseExpr.asVectorProcessor(inspector)
      );
    }
    return (ExprVectorProcessor<T>) processor;
  }

  public static <T> ExprVectorProcessor<T> caseSearchedFunction(
      Expr.VectorInputBindingInspector inspector,
      List<Expr> args
  )
  {
    final int conditionProcessorsCount = (int) (double) (args.size() / 2);
    final int thenProcessorsCount = args.size() - conditionProcessorsCount;
    final ExprVectorProcessor<?>[] conditionProcessors = new ExprVectorProcessor<?>[conditionProcessorsCount];
    final ExprVectorProcessor[] thenProcessors = new ExprVectorProcessor[thenProcessorsCount];

    ExpressionType outputType = null;
    for (int i = 0, j = 0, k = 0; i < args.size(); i++) {
      // right now canVectorize verifies that all args have the same output type, but this is aspirational towards what
      // the logic probably should be and is using least restrictive type. if the canVectorize logic changes to
      // something other than least restrictive type, then so should this
      if ((i % 2) == 0 && j < conditionProcessorsCount) {
        conditionProcessors[j++] = args.get(i).asVectorProcessor(inspector);
      } else {
        outputType = ExpressionTypeConversion.leastRestrictiveType(outputType, args.get(i).getOutputType(inspector));
        thenProcessors[k++] = args.get(i).asVectorProcessor(inspector);
      }
    }

    final ExprVectorProcessor<?> processor;
    if (outputType == null) {
      // if output type is null, it means all the input types were null (non-existent), and if(null, null, null) is null
      return VectorProcessors.constant((Long) null, inspector.getMaxVectorSize());
    }
    if (outputType.is(ExprType.LONG)) {
      // long is most restrictive so both processors are definitely long typed if output is long
      processor = new CaseSearchedLongVectorProcessor(
          conditionProcessors,
          thenProcessors
      );
    } else if (outputType.is(ExprType.DOUBLE)) {
      processor = new CaseSearchedDoubleVectorProcessor(
          conditionProcessors,
          thenProcessors
      );
    } else {
      processor = new CaseSearchedObjectVectorProcessor(
          outputType,
          conditionProcessors,
          thenProcessors
      );
    }
    return (ExprVectorProcessor<T>) processor;
  }
}
