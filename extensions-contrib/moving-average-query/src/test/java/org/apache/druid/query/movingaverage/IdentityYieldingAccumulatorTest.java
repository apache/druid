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

package org.apache.druid.query.movingaverage;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test IdentityYieldingAccumulator
 */
public class IdentityYieldingAccumulatorTest
{
  @Test
  public void testAccumulator()
  {
    Sequence<Integer> seq = Sequences.simple(Arrays.asList(1, 2, 3, 4, 5));

    Yielder<Integer> y = seq.toYielder(null, new IdentityYieldingAccumulator<>());

    assertEquals(Integer.valueOf(1), y.get());
    y = y.next(null);
    assertEquals(Integer.valueOf(2), y.get());
    y = y.next(null);
    assertEquals(Integer.valueOf(3), y.get());
    y = y.next(null);
    assertEquals(Integer.valueOf(4), y.get());
    y = y.next(null);
    assertEquals(Integer.valueOf(5), y.get());
    y = y.next(null);
    assertTrue(y.isDone());

    assertNull(y.get());
  }
}
