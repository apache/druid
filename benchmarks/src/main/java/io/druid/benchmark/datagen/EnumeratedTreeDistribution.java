/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.datagen;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.TreeMap;

/**
 * EnumeratedDistrubtion's sample() method does a linear scan through the array of probabilities.
 *
 * This is too slow with high cardinality value sets, so this subclass overrides sample() to use
 * a TreeMap instead.
 */
public class EnumeratedTreeDistribution<T> extends EnumeratedDistribution
{
  private TreeMap<Double, Integer> probabilityRanges;
  private List<Pair<T, Double>> normalizedPmf;

  public EnumeratedTreeDistribution(final List<Pair<T, Double>> pmf)
  {
    super(pmf);

    // build the interval tree
    probabilityRanges = new TreeMap<Double, Integer>();
    normalizedPmf = this.getPmf();
    double cumulativep = 0.0;
    for (int i = 0; i < normalizedPmf.size(); i++) {
      probabilityRanges.put(cumulativep, i);
      Pair<T, Double> pair = normalizedPmf.get(i);
      cumulativep += pair.getSecond();
    }
  }

  @Override
  public T sample()
  {
    final double randomValue = random.nextDouble();
    Integer valueIndex = probabilityRanges.floorEntry(randomValue).getValue();
    return normalizedPmf.get(valueIndex).getFirst();
  }
}
