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

import static org.junit.Assert.assertEquals;

/**
 * Use a mock operator to test (and illustrate) the basic operator
 * mechanisms.
 */
@Category(OperatorTest.class)
public class MockOperatorTest
{
  @Test
  public void testMockStringOperator() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<String> op = MockOperator.strings(fragment, 2);
    fragment.registerRoot(op);
    assertEquals(Arrays.asList("Mock row 0", "Mock row 1"), fragment.toList());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testMockIntOperator()
  {
    // Test using the toList feature.
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 2);
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 1), fragment.toList());
    assertEquals(Operator.State.CLOSED, op.state);
  }
}
