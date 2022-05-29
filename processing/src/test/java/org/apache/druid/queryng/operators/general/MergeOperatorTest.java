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

package org.apache.druid.queryng.operators.general;

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.MockOperator;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.State;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.ResultIterator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class MergeOperatorTest
{
  @Test
  public void testNoInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new MergeOperator<Integer>(
        fragment,
        Ordering.natural(),
        Collections.emptyList()
    );
    fragment.registerRoot(op);
    assertTrue(fragment.toList().isEmpty());
  }

  @Test
  public void testEmptyInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new MergeOperator<>(
        fragment,
        Ordering.natural(),
        Arrays.asList(
            new NullOperator<Integer>(fragment),
            new NullOperator<Integer>(fragment)
        )
    );
    fragment.registerRoot(op);
    assertTrue(fragment.toList().isEmpty());
  }

  @Test
  public void testOneInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new MergeOperator<>(
        fragment,
        Ordering.natural(),
        Collections.singletonList(MockOperator.ints(fragment, 3))
    );
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 1, 2), fragment.toList());
  }

  @Test
  public void testTwoInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new MergeOperator<>(
        fragment,
        Ordering.natural(),
        Arrays.asList(
            MockOperator.ints(fragment, 3),
            MockOperator.ints(fragment, 5)
        )
    );
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 0, 1, 1, 2, 2, 3, 4), fragment.toList());
  }

  @Test
  public void testClose()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> input1 = MockOperator.ints(fragment, 2);
    MockOperator<Integer> input2 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = new MergeOperator<>(
        fragment,
        Ordering.natural(),
        Arrays.asList(input1, input2)
    );
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 0, 1, 1), fragment.toList());

    // Inputs are closed as exhausted.
    assertTrue(input1.state == State.CLOSED);
    assertTrue(input2.state == State.CLOSED);
    op.close(true);
  }

  @Test
  public void testEarlyClose() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> input1 = MockOperator.ints(fragment, 2);
    MockOperator<Integer> input2 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = new MergeOperator<>(
        fragment,
        Ordering.natural(),
        Arrays.asList(input1, input2)
    );
    fragment.registerRoot(op);
    assertTrue(fragment.toList().isEmpty());

    // Inputs are closed as exhausted.
    assertTrue(input1.state == State.CLOSED);
    assertTrue(input2.state == State.CLOSED);
  }
}
