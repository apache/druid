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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceTestHelper;
import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentHandle;
import org.apache.druid.queryng.fragment.FragmentRun;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the various fragment-level classes: {@code FragmentBuilder},
 * {@link FragmentHandle} and {@link FragmentRun}. Since the second two
 * are convenience wrappers around the first, each test uses the wrappers
 * to indirectly test the builder.
 */
public class FragmentTest
{
  @Test
  public void testOperatorBasics()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);

    // Handle for the leaf operator
    FragmentHandle<Integer> handle = builder.handle(op);
    assertTrue(handle.rootIsOperator());
    assertFalse(handle.rootIsSequence());
    assertSame(op, handle.rootOperator());
    assertNull(handle.rootSequence());
    assertSame(handle, handle.toOperator());
    assertSame(builder.context(), handle.context());

    // Add another operator to the DAG.
    Operator<Integer> op2 = new MockFilterOperator<Integer>(builder.context(), op, x -> x < 3);
    FragmentHandle<Integer> handle2 = handle.compose(op2);
    assertTrue(handle2.rootIsOperator());
    assertFalse(handle2.rootIsSequence());
    assertSame(op2, handle2.rootOperator());
    assertNull(handle2.rootSequence());
    assertSame(handle2, handle2.toOperator());
    assertSame(builder.context(), handle2.context());
  }

  /**
   * The empty handle is a "bootstrap" mechanism for building the
   * leaf-most operator in a DAG.
   */
  @Test
  public void testEmptyHandle()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    FragmentHandle<Object> emptyHandle = builder.emptyHandle();

    assertFalse(emptyHandle.rootIsOperator());
    assertFalse(emptyHandle.rootIsSequence());
    assertNull(emptyHandle.rootOperator());
    assertNull(emptyHandle.rootSequence());
    assertSame(emptyHandle, emptyHandle.toOperator());
    assertSame(builder.context(), emptyHandle.context());

    // There is no useful case for converting an empty handle
    // to a sequence.
    try {
      emptyHandle.toSequence();
      fail();
    }
    catch (ISE e) {
      // Expected
    }

    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    FragmentHandle<Integer> handle = emptyHandle.compose(op);
    assertTrue(handle.rootIsOperator());
    assertSame(op, handle.rootOperator());
    assertSame(builder.context(), handle.context());
  }

  @Test
  public void testSequenceBasics()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    Sequence<Integer> seq = Operators.toSequence(op);

    // Handle for the leaf sequence
    FragmentHandle<Integer> handle = builder.handle(seq);
    assertFalse(handle.rootIsOperator());
    assertTrue(handle.rootIsSequence());
    // Note asymmetry: sequences can be unwrapped to get an operator,
    // but not visa-versa.
    assertSame(op, handle.rootOperator());
    assertSame(seq, handle.rootSequence());
    assertSame(handle, handle.toSequence());
    assertSame(builder.context(), handle.context());

    // Add another operator to the DAG by unwrapping the above
    // sequence.
    Operator<Integer> op2 = new MockFilterOperator<Integer>(
        builder.context(),
        handle.toOperator().rootOperator(),
        x -> x < 3);
    FragmentHandle<Integer> handle2o = handle.compose(op2);

    // Use composition to get the new root sequence.
    FragmentHandle<Integer> handle2 = handle2o.toSequence();
    assertFalse(handle2.rootIsOperator());
    assertTrue(handle2.rootIsSequence());
    assertSame(op2, handle2.rootOperator());
    assertNotNull(handle2.rootSequence());
    assertSame(handle2, handle2.toSequence());
    assertSame(builder.context(), handle2.context());
  }

  @Test
  public void testRun()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    FragmentHandle<Integer> handle = builder.handle(op);
    int i = 0;
    try (FragmentRun<Integer> run = handle.run()) {
      for (Integer value : run) {
        assertEquals(i++, (int) value);
      }
    }
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testToListWithOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    FragmentHandle<Integer> handle = builder.handle(op);
    List<Integer> results = handle.run().toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Test running a DAG with a root sequence as a sequence.
   */
  @Test
  public void testToListWithSequence()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    FragmentHandle<Integer> handle = builder.handle(op).toSequence();
    List<Integer> results = handle.runAsSequence().toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Test running a fragment as a sequence when the root is an
   * operator. The operator will be wrapped in a sequence internally.
   */
  @Test
  public void testRunAsSequenceWithOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    FragmentHandle<Integer> handle = builder.handle(op);
    List<Integer> results = handle.runAsSequence().toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Test that if an operator stack has a sequence as its root,
   * that running the DAG as an operator will unwrap that root
   * sequence to get an operator.
   */
  @Test
  public void testRunWithSequence()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    FragmentHandle<Integer> handle = builder.handle(op).toSequence();
    List<Integer> results = handle.run().toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * An operator is a one-pass object, don't try sequence tests that assume
   * the sequence is reentrant.
   */
  @Test
  public void testSequenceYielder() throws IOException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 5);
    final List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4);
    Sequence<Integer> seq = builder.runAsSequence(op);
    SequenceTestHelper.testYield("op", 5, seq, expected);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testSequenceAccum() throws IOException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    final List<Integer> vals = Arrays.asList(0, 1, 2, 3);
    FragmentHandle<Integer> handle = builder.handle(op);
    Sequence<Integer> seq = handle.runAsSequence();
    SequenceTestHelper.testAccumulation("op", seq, vals);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testRunEmptyHandle()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    FragmentHandle<Object> emptyHandle = builder.emptyHandle();
    assertTrue(emptyHandle.run().toList().isEmpty());
  }

  @Test
  public void testRunEmptyHandleAsSequence()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    FragmentHandle<Integer> emptyHandle = builder.emptyHandle();
    Sequence<Integer> seq = emptyHandle.runAsSequence();
    SequenceTestHelper.testAccumulation("empty", seq, Collections.emptyList());
  }
}
