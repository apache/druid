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
import org.apache.druid.query.aggregation.LongSumVectorAggregator;
import org.apache.druid.query.aggregation.NullAwareVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.nio.ByteBuffer;

/**
 * SIMD specialization of {@link LongSumVectorAggregator}'s ungrouped contiguous-range aggregation. The hot loop
 * issues a hardcoded {@link LongVector#add} and a {@code reduceLanes(VectorOperators.ADD)} so the JIT emits the
 * platform's long-add and long-add-reduce intrinsics. Null lanes are skipped via {@link VectorMask}.
 *
 * The grouped scatter-gather variant is inherited from the parent (scalar) class.
 */
public final class SimdLongSumVectorAggregator extends LongSumVectorAggregator implements NullAwareVectorAggregator
{
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  private final VectorValueSelector selector;

  public SimdLongSumVectorAggregator(VectorValueSelector selector)
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
    LongVector vacc = LongVector.zero(SPECIES);
    for (; i < upperBound; i += laneCount) {
      vacc = vacc.add(LongVector.fromArray(SPECIES, vector, i));
    }
    long sum = vacc.reduceLanes(VectorOperators.ADD);
    for (; i < endRow; i++) {
      sum += vector[i];
    }
    buf.putLong(position, buf.getLong(position) + sum);
  }

  @Override
  public boolean aggregate(ByteBuffer buf, int position, int startRow, int endRow, boolean[] nullVector)
  {
    final long[] vector = selector.getLongVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    LongVector vacc = LongVector.zero(SPECIES);
    int nonNullCount = 0;
    for (; i < upperBound; i += laneCount) {
      final VectorMask<Long> notNull = VectorMask.fromArray(SPECIES, nullVector, i).not();
      vacc = vacc.add(LongVector.fromArray(SPECIES, vector, i), notNull);
      nonNullCount += notNull.trueCount();
    }
    long sum = vacc.reduceLanes(VectorOperators.ADD);
    for (; i < endRow; i++) {
      if (!nullVector[i]) {
        sum += vector[i];
        nonNullCount++;
      }
    }
    if (nonNullCount > 0) {
      buf.putLong(position, buf.getLong(position) + sum);
      return true;
    }
    return false;
  }
}
