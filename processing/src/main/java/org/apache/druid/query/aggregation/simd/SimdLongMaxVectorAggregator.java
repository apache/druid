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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.query.aggregation.LongMaxVectorAggregator;
import org.apache.druid.query.aggregation.NullAwareVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.nio.ByteBuffer;

/**
 * SIMD specialization of {@link LongMaxVectorAggregator}'s ungrouped contiguous-range aggregation. The hot loop
 * issues a hardcoded {@link LongVector#max} and a {@code reduceLanes(VectorOperators.MAX)} so the JIT emits the
 * platform's long-max and long-max-reduce intrinsics. Null lanes preserve the lane's seeded {@link Long#MIN_VALUE}
 * via masked {@code lanewise} so the reduction is unaffected by them.
 */
public final class SimdLongMaxVectorAggregator extends LongMaxVectorAggregator implements NullAwareVectorAggregator
{
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  private final VectorValueSelector selector;

  public SimdLongMaxVectorAggregator(VectorValueSelector selector)
  {
    super(selector);
    this.selector = selector;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final long[] vector = selector.getLongVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    LongVector vacc = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
    for (; i < upperBound; i += laneCount) {
      vacc = vacc.max(LongVector.fromArray(SPECIES, vector, i));
    }
    long localMax = vacc.reduceLanes(VectorOperators.MAX);
    for (; i < endRow; i++) {
      localMax = Math.max(localMax, vector[i]);
    }
    buf.putLong(position, Math.max(buf.getLong(position), localMax));
  }

  @Override
  public boolean aggregate(ByteBuffer buf, int position, int startRow, int endRow, boolean[] nullVector)
  {
    final long[] vector = selector.getLongVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    LongVector vacc = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
    int nonNullCount = 0;
    for (; i < upperBound; i += laneCount) {
      final VectorMask<Long> notNull = VectorMask.fromArray(SPECIES, nullVector, i).not();
      vacc = vacc.lanewise(VectorOperators.MAX, LongVector.fromArray(SPECIES, vector, i), notNull);
      nonNullCount += notNull.trueCount();
    }
    long localMax = vacc.reduceLanes(VectorOperators.MAX);
    for (; i < endRow; i++) {
      if (!nullVector[i]) {
        localMax = Math.max(localMax, vector[i]);
        nonNullCount++;
      }
    }
    if (nonNullCount > 0) {
      buf.putLong(position, Math.max(buf.getLong(position), localMax));
      return true;
    }
    return false;
  }
}
