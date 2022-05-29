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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class PushBackOperatorTest
{
  @Test
  public void testEmptyInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<String> input = new NullOperator<String>(fragment);
    Operator<String> op = new PushBackOperator<String>(fragment, input);
    fragment.registerRoot(op);
    List<String> results = fragment.toList();
    assertTrue(results.isEmpty());
  }

  @Test
  public void testSimpleInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input = MockOperator.ints(fragment, 2);
    Operator<Integer> op = new PushBackOperator<Integer>(fragment, input);
    fragment.registerRoot(op);
    List<Integer> results = fragment.toList();
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testPush() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input = MockOperator.ints(fragment, 2);
    PushBackOperator<Integer> op = new PushBackOperator<Integer>(fragment, input);
    fragment.registerRoot(op);
    ResultIterator<Integer> iter = fragment.run();
    Integer item = iter.next();
    op.push(item);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
    fragment.close();
  }

  @Test
  public void testInitialPush() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> input = MockOperator.ints(fragment, 2);
    ResultIterator<Integer> iter = input.open();
    Integer item = iter.next();
    PushBackOperator<Integer> op = new PushBackOperator<Integer>(fragment, input, iter, item);
    fragment.registerRoot(op);
    List<Integer> results = fragment.toList();
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }
}
