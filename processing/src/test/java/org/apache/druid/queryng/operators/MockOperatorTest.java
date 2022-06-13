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

package org.apache.druid.queryng.operators;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentHandle;
import org.apache.druid.queryng.fragment.FragmentRun;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Use a mock operator to test (and illustrate) the basic operator
 * mechanisms.
 */
public class MockOperatorTest
{
  @Test
  public void testMockStringOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<String> op = MockOperator.strings(builder.context(), 2);
    FragmentHandle<String> handle = builder.handle(op);
    try (FragmentRun<String> run = handle.run()) {
      Iterator<String> iter = run.iterator();
      assertTrue(iter.hasNext());
      assertEquals("Mock row 0", iter.next());
      assertTrue(iter.hasNext());
      assertEquals("Mock row 1", iter.next());
      assertFalse(iter.hasNext());
    }
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testMockIntOperator()
  {
    // Test using the toList feature.
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    List<Integer> results = builder.run(op).toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Getting weird: an operator that wraps a sequence that wraps an operator.
   */
  @Test
  public void testSequenceOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<String> op = MockOperator.strings(builder.context(), 2);
    Sequence<String> seq = Operators.toSequence(op);
    Operator<String> outer = Operators.toOperator(builder, seq);
    FragmentRun<String> run = builder.run(outer);
    Iterator<String> iter = run.iterator();
    assertTrue(iter.hasNext());
    assertEquals("Mock row 0", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("Mock row 1", iter.next());
    assertFalse(iter.hasNext());
    run.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testMockFilter()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    Operator<Integer> op2 = new MockFilterOperator<Integer>(
        builder.context(),
        op,
        x -> x % 2 == 0);
    List<Integer> results = builder.run(op2).toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(2, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }
}
