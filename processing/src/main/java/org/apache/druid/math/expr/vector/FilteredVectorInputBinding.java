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
import org.apache.druid.query.filter.vector.VectorMatch;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class FilteredVectorInputBinding implements Expr.VectorInputBinding
{
  private Expr.VectorInputBinding delegate;
  // caches of re-usable vectors
  private final Map<String, Object[]> cachedObjects;
  private final Map<String, long[]> cachedLongs;
  private final Map<String, double[]> cachedDoubles;
  private final Map<String, boolean[]> cachedNulls;
  private final VectorMatch vectorMatch;

  public FilteredVectorInputBinding(int maxVectorSize)
  {
    final int[] match = new int[maxVectorSize];
    for (int i = 0; i < maxVectorSize; ++i) {
      match[i] = i;
    }
    vectorMatch = VectorMatch.wrap(match);
    // these caches exist so we do not need to allocate new vectors for each time the match changes, but they are not
    // currently wired up as a true cache for re-using the values for calls to the get vector methods (so they
    // recompute into re-used arrays for every call to the get methods). This is fine for current usage patterns, where
    // we call the filtered bindings once per input per match, but if this ever changes we might want to consider
    // caching per match too. this would involve moving match manipulation inside of this class instead of allowing
    // callers to do it with getVectorMatch
    this.cachedObjects = new HashMap<>();
    this.cachedLongs = new HashMap<>();
    this.cachedDoubles = new HashMap<>();
    this.cachedNulls = new HashMap<>();
  }

  public void setBindings(Expr.VectorInputBinding bindings)
  {
    this.delegate = bindings;
  }

  public VectorMatch getVectorMatch()
  {
    return vectorMatch;
  }

  @Nullable
  @Override
  public ExpressionType getType(String name)
  {
    return delegate.getType(name);
  }

  @Override
  public int getMaxVectorSize()
  {
    return delegate.getMaxVectorSize();
  }

  @Override
  public Object[] getObjectVector(String name)
  {
    final Object[] baseVector = delegate.getObjectVector(name);
    final Object[] matchVector = cachedObjects.computeIfAbsent(name, k -> new Object[delegate.getMaxVectorSize()]);
    final int[] selection = vectorMatch.getSelection();
    for (int i = 0; i < vectorMatch.getSelectionSize(); i++) {
      matchVector[i] = baseVector[selection[i]];
    }
    return matchVector;
  }

  @Override
  public long[] getLongVector(String name)
  {
    final long[] baseVector = delegate.getLongVector(name);
    final long[] matchVector = cachedLongs.computeIfAbsent(name, k -> new long[delegate.getMaxVectorSize()]);
    final int[] selection = vectorMatch.getSelection();
    for (int i = 0; i < vectorMatch.getSelectionSize(); i++) {
      matchVector[i] = baseVector[selection[i]];
    }
    return matchVector;
  }

  @Override
  public double[] getDoubleVector(String name)
  {
    final double[] baseVector = delegate.getDoubleVector(name);
    final double[] matchVector = cachedDoubles.computeIfAbsent(name, k -> new double[delegate.getMaxVectorSize()]);
    final int[] selection = vectorMatch.getSelection();
    for (int i = 0; i < vectorMatch.getSelectionSize(); i++) {
      matchVector[i] = baseVector[selection[i]];
    }
    return matchVector;
  }

  @Nullable
  @Override
  public boolean[] getNullVector(String name)
  {
    final boolean[] baseVector = delegate.getNullVector(name);
    if (baseVector == null) {
      return null;
    }
    final boolean[] matchVector = cachedNulls.computeIfAbsent(name, k -> new boolean[delegate.getMaxVectorSize()]);
    final int[] selection = vectorMatch.getSelection();
    for (int i = 0; i < vectorMatch.getSelectionSize(); i++) {
      matchVector[i] = baseVector[selection[i]];
    }
    return matchVector;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return vectorMatch.getSelectionSize();
  }

  @Override
  public int getCurrentVectorId()
  {
    return delegate.getCurrentVectorId();
  }
}
