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

package org.apache.druid.query.aggregation.simd;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.query.aggregation.FloatMaxVectorAggregator;
import org.apache.druid.query.aggregation.NullAwareVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.nio.ByteBuffer;

/**
 * SIMD specialization of {@link FloatMaxVectorAggregator}'s ungrouped contiguous-range aggregation. The hot loop
 * issues a hardcoded {@link FloatVector#max} and a {@code reduceLanes(VectorOperators.MAX)} so the JIT emits the
 * platform's float-max and float-max-reduce intrinsics. Null lanes preserve the lane's seeded
 * {@link Float#NEGATIVE_INFINITY} via masked {@code lanewise} so the reduction is unaffected by them.
 */
public final class SimdFloatMaxVectorAggregator extends FloatMaxVectorAggregator implements NullAwareVectorAggregator
{
  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

  private final VectorValueSelector selector;

  public SimdFloatMaxVectorAggregator(VectorValueSelector selector)
  {
    super(selector);
    this.selector = selector;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final float[] vector = selector.getFloatVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    FloatVector vacc = FloatVector.broadcast(SPECIES, Float.NEGATIVE_INFINITY);
    for (; i < upperBound; i += laneCount) {
      vacc = vacc.max(FloatVector.fromArray(SPECIES, vector, i));
    }
    float localMax = vacc.reduceLanes(VectorOperators.MAX);
    for (; i < endRow; i++) {
      localMax = Math.max(localMax, vector[i]);
    }
    buf.putFloat(position, Math.max(buf.getFloat(position), localMax));
  }

  @Override
  public boolean aggregate(ByteBuffer buf, int position, int startRow, int endRow, boolean[] nullVector)
  {
    final float[] vector = selector.getFloatVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    FloatVector vacc = FloatVector.broadcast(SPECIES, Float.NEGATIVE_INFINITY);
    int nonNullCount = 0;
    for (; i < upperBound; i += laneCount) {
      final VectorMask<Float> notNull = VectorMask.fromArray(SPECIES, nullVector, i).not();
      vacc = vacc.lanewise(VectorOperators.MAX, FloatVector.fromArray(SPECIES, vector, i), notNull);
      nonNullCount += notNull.trueCount();
    }
    float localMax = vacc.reduceLanes(VectorOperators.MAX);
    for (; i < endRow; i++) {
      if (!nullVector[i]) {
        localMax = Math.max(localMax, vector[i]);
        nonNullCount++;
      }
    }
    if (nonNullCount > 0) {
      buf.putFloat(position, Math.max(buf.getFloat(position), localMax));
      return true;
    }
    return false;
  }
}
