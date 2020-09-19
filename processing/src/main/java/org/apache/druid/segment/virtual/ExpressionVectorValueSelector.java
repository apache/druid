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

package org.apache.druid.segment.virtual;

import com.google.common.base.Preconditions;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class ExpressionVectorValueSelector implements VectorValueSelector
{
  final Expr.VectorInputBinding bindings;
  final ExprVectorProcessor<?> processor;
  final float[] floats;

  public ExpressionVectorValueSelector(ExprVectorProcessor<?> processor, Expr.VectorInputBinding bindings)
  {
    this.processor = Preconditions.checkNotNull(processor, "processor");
    this.bindings = Preconditions.checkNotNull(bindings, "bindings");
    this.floats = new float[bindings.getMaxVectorSize()];
  }

  @Override
  public long[] getLongVector()
  {
    return processor.evalVector(bindings).getLongVector();
  }

  @Override
  public float[] getFloatVector()
  {
    final double[] doubles = processor.evalVector(bindings).getDoubleVector();
    for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
      floats[i] = (float) doubles[i];
    }
    return floats;
  }

  @Override
  public double[] getDoubleVector()
  {
    return processor.evalVector(bindings).getDoubleVector();
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    return processor.evalVector(bindings).getNullVector();
  }

  @Override
  public int getMaxVectorSize()
  {
    return bindings.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return bindings.getCurrentVectorSize();
  }
}
