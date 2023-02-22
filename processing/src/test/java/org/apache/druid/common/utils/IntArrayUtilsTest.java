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

package org.apache.druid.common.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IntArrayUtilsTest
{
  @Test
  public void testInverse()
  {
    final int numVals = 10000;
    final Random random = ThreadLocalRandom.current();
    final int[] inverted = new int[numVals];
    final int[] original = new int[numVals];

    final List<Integer> ints = IntStream.range(0, numVals).boxed().collect(Collectors.toList());
    Collections.shuffle(ints, random);

    for (int i = 0; i < numVals; i++) {
      inverted[i] = ints.get(i);
      original[i] = inverted[i];
    }
    IntArrayUtils.inverse(inverted);

    for (int i = 0; i < numVals; i++) {
      Assert.assertEquals(i, inverted[original[i]]);
    }
  }
}
