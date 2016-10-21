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

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.AbstractRealDistribution;

/*
 * Rounds the output values from the sample() function of an AbstractRealDistribution.
 */
public class RealRoundingDistribution extends AbstractIntegerDistribution
{
  private AbstractRealDistribution realDist;

  public RealRoundingDistribution(AbstractRealDistribution realDist)
  {
    this.realDist = realDist;
  }

  @Override
  public double probability(int x)
  {
    return 0;
  }

  @Override
  public double cumulativeProbability(int x)
  {
    return 0;
  }

  @Override
  public double getNumericalMean()
  {
    return 0;
  }

  @Override
  public double getNumericalVariance()
  {
    return 0;
  }

  @Override
  public int getSupportLowerBound()
  {
    return 0;
  }

  @Override
  public int getSupportUpperBound()
  {
    return 0;
  }

  @Override
  public boolean isSupportConnected()
  {
    return false;
  }

  @Override
  public void reseedRandomGenerator(long seed)
  {
    realDist.reseedRandomGenerator(seed);
  }

  @Override
  public int sample()
  {
    double randomVal = realDist.sample();
    Long longVal = Math.round(randomVal);
    return longVal.intValue();
  }
}
