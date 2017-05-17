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

import java.util.Collections;
import java.util.List;

public class SequentialDistribution extends EnumeratedDistribution
{

  private Integer start;
  private Integer end;
  private List<Object> enumeratedValues;
  private int counter;


  public SequentialDistribution(Integer start, Integer end, List<Object> enumeratedValues)
  {
    // just pass in some bogus probability mass function, we won't be using it
    super(Collections.singletonList(new Pair<Object, Double>(null, 1.0)));
    this.start = start;
    this.end = end;
    this.enumeratedValues = enumeratedValues;
    if (enumeratedValues == null) {
      counter = start;
    } else {
      counter = 0;
    }
  }

  @Override
  public Object sample()
  {
    Object ret;
    if (enumeratedValues != null) {
      ret = enumeratedValues.get(counter);
      counter = (counter + 1) % enumeratedValues.size();
    } else {
      ret = counter;
      counter++;
      if (counter >= end) {
        counter = start;
      }
    }
    return ret;
  }
}
