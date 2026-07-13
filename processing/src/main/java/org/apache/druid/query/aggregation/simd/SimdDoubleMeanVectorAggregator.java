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

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.query.aggregation.NullAwareVectorAggregator;
import org.apache.druid.query.aggregation.mean.DoubleMeanHolder;
import org.apache.druid.query.aggregation.mean.DoubleMeanVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.nio.ByteBuffer;

/**
 * SIMD specialization of {@link DoubleMeanVectorAggregator}'s ungrouped contiguous-range aggregation. The hot loop
 * reduces input values into one local sum and count, then updates the mean holder once. The grouped scatter-gather
 * variant is inherited from the parent scalar class.
 */
public final class SimdDoubleMeanVectorAggregator extends DoubleMeanVectorAggregator implements NullAwareVectorAggregator
{
  private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

  private final VectorValueSelector selector;

  public SimdDoubleMeanVectorAggregator(final VectorValueSelector selector)
  {
    super(selector);
    this.selector = selector;
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final boolean[] nullVector = selector.getNullVector();
    if (nullVector == null) {
      aggregateNoNulls(buf, position, startRow, endRow);
    } else {
      aggregate(buf, position, startRow, endRow, nullVector);
    }
  }

  @Override
  public boolean aggregate(
      final ByteBuffer buf,
      final int position,
      final int startRow,
      final int endRow,
      final boolean[] nullVector
  )
  {
    final double[] vector = selector.getDoubleVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    DoubleVector vacc = DoubleVector.zero(SPECIES);
    long count = 0;
    for (; i < upperBound; i += laneCount) {
      final VectorMask<Double> notNull = VectorMask.fromArray(SPECIES, nullVector, i).not();
      vacc = vacc.add(DoubleVector.fromArray(SPECIES, vector, i), notNull);
      count += notNull.trueCount();
    }
    double sum = vacc.reduceLanes(VectorOperators.ADD);
    for (; i < endRow; i++) {
      if (!nullVector[i]) {
        sum += vector[i];
        count++;
      }
    }
    if (count > 0) {
      DoubleMeanHolder.update(buf, position, sum, count);
      return true;
    }
    return false;
  }

  private void aggregateNoNulls(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final double[] vector = selector.getDoubleVector();

    final int laneCount = SPECIES.length();
    final int upperBound = startRow + SPECIES.loopBound(endRow - startRow);
    int i = startRow;
    DoubleVector vacc = DoubleVector.zero(SPECIES);
    for (; i < upperBound; i += laneCount) {
      vacc = vacc.add(DoubleVector.fromArray(SPECIES, vector, i));
    }
    double sum = vacc.reduceLanes(VectorOperators.ADD);
    for (; i < endRow; i++) {
      sum += vector[i];
    }
    DoubleMeanHolder.update(buf, position, sum, endRow - startRow);
  }
}
