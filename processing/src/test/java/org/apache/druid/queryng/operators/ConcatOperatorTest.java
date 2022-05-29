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

import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.Operator.State;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class ConcatOperatorTest
{
  @Test
  public void testEmpty()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<String> op = new ConcatOperator<String>(fragment, Collections.emptyList());
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    assertTrue(results.isEmpty());
  }

  @Test
  public void testOneEmptyInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<String> input = new NullOperator<String>(fragment);
    Operator<String> op = new ConcatOperator<String>(
        fragment,
        Collections.singletonList(input));
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    assertTrue(results.isEmpty());
  }

  @Test
  public void testHelperNoConcat()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> input1 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(fragment, Collections.singletonList(input1));
    assertSame(input1, op);
  }

  @Test
  public void testTwoEmptyInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<String> input1 = new NullOperator<String>(fragment);
    Operator<String> input2 = new NullOperator<String>(fragment);
    Operator<String> op = new ConcatOperator<String>(
        fragment,
        Arrays.asList(input1, input2));
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    assertTrue(results.isEmpty());
  }

  @Test
  public void testOneInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input = MockOperator.ints(fragment, 2);
    Operator<Integer> op = new ConcatOperator<Integer>(
        fragment,
        Collections.singletonList(input));
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testEmptyThenNonEmptyInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input1 = new NullOperator<Integer>(fragment);
    Operator<Integer> input2 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        fragment,
        Arrays.asList(input1, input2));
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testNonEmptyThenEmptyInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input1 = MockOperator.ints(fragment, 2);
    Operator<Integer> input2 = new NullOperator<Integer>(fragment);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        fragment,
        Arrays.asList(input1, input2));
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testTwoInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input1 = MockOperator.ints(fragment, 2);
    Operator<Integer> input2 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        fragment,
        Arrays.asList(input1, input2));
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    List<Integer> expected = Arrays.asList(0, 1, 0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testClose() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> input1 = MockOperator.ints(fragment, 2);
    MockOperator<Integer> input2 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        fragment,
        Arrays.asList(input1, input2));
    fragment.registerRoot(op);
    ResultIterator<Integer> iter = fragment.run();
    assertEquals(0, (int) iter.next());

    // Only first input has been opened.
    assertEquals(State.RUN, input1.state);
    assertEquals(State.START, input2.state);

    // Cascade closes inputs
    op.close(true);
    assertEquals(State.CLOSED, input1.state);
    assertEquals(State.START, input2.state);

    // Close again does nothing.
    op.close(false);
  }
}
