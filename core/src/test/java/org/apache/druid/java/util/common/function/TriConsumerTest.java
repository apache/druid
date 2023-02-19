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

package org.apache.druid.java.util.common.function;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class TriConsumerTest
{

  @Test
  public void sanityTest()
  {
    Set<Integer> sumSet = new HashSet<>();
    TriConsumer<Integer, Integer, Integer> consumerA = (arg1, arg2, arg3) -> {
      sumSet.add(arg1 + arg2 + arg3);
    };
    TriConsumer<Integer, Integer, Integer> consumerB = (arg1, arg2, arg3) -> {
      sumSet.remove(arg1 + arg2 + arg3);
    };
    consumerA.andThen(consumerB).accept(1, 2, 3);

    Assert.assertTrue(sumSet.isEmpty());
  }
}
