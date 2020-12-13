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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

class ExpressionVectorInputBinding implements Expr.VectorInputBinding
{
  private final Map<String, VectorValueSelector> numeric;
  private final Map<String, VectorObjectSelector> objects;
  private final Map<String, ExprType> types;
  private final NilVectorSelector nilSelector;

  private final ReadableVectorInspector vectorInspector;

  public ExpressionVectorInputBinding(ReadableVectorInspector vectorInspector)
  {
    this.numeric = new HashMap<>();
    this.objects = new HashMap<>();
    this.types = new HashMap<>();
    this.vectorInspector = vectorInspector;
    this.nilSelector = NilVectorSelector.create(this.vectorInspector);
  }

  public ExpressionVectorInputBinding addNumeric(String name, ExprType type, VectorValueSelector selector)
  {
    numeric.put(name, selector);
    types.put(name, type);
    return this;
  }

  public ExpressionVectorInputBinding addObjectSelector(String name, ExprType type, VectorObjectSelector selector)
  {
    objects.put(name, selector);
    types.put(name, type);
    return this;
  }

  @Override
  public ExprType getType(String name)
  {
    return types.get(name);
  }

  @Override
  public <T> T[] getObjectVector(String name)
  {
    return (T[]) objects.getOrDefault(name, nilSelector).getObjectVector();
  }

  @Override
  public long[] getLongVector(String name)
  {
    return numeric.getOrDefault(name, nilSelector).getLongVector();
  }

  @Override
  public double[] getDoubleVector(String name)
  {
    return numeric.getOrDefault(name, nilSelector).getDoubleVector();
  }

  @Nullable
  @Override
  public boolean[] getNullVector(String name)
  {
    return numeric.getOrDefault(name, nilSelector).getNullVector();
  }

  @Override
  public int getMaxVectorSize()
  {
    return vectorInspector.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return vectorInspector.getCurrentVectorSize();
  }

  @Override
  public int getCurrentVectorId()
  {
    return vectorInspector.getId();
  }
}
