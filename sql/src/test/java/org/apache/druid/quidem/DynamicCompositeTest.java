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

package org.apache.druid.quidem;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class DynamicCompositeTest
{
  @Test
  public void testCompose()
  {
    HashSet<Integer> set = new HashSet<Integer>();
    Function<Integer, Integer> sq = x -> x * x;
    Set<Integer> composite = DynamicComposite.make(set, Set.class, sq, Function.class);
    composite.add(1);
    assertEquals(1, set.size());
    assertEquals(1, composite.size());

    assertInstanceOf(Function.class, composite);
    Function<Integer, Integer> sq2 = (Function<Integer, Integer>) composite;
    assertEquals(9, sq2.apply(3));
  }
}
