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

package org.apache.druid.segment.serde;

import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.junit.Test;

/**
 * Test for {@link CombineFirstTwoValuesColumnarInts}.
 */
public class CombineFirstTwoValuesColumnarIntsTest
{
  @Test
  public void testCombineFirstTwoValues()
  {
    // (expectedCombined, original)
    assertCombine(new int[]{0, 1, 2}, new int[]{1, 2, 3});
    assertCombine(new int[]{0, 0, 1, 2}, new int[]{0, 1, 2, 3});
    assertCombine(new int[]{2, 0, 1, 0, 4, 0}, new int[]{3, 0, 2, 1, 5, 0});
  }

  private static void assertCombine(final int[] expectedCombined, final int[] original)
  {
    CombineFirstTwoValuesIndexedIntsTest.assertCombine(
        expectedCombined,
        original,
        arr -> new CombineFirstTwoValuesIndexedInts(new ArrayBasedIndexedInts(arr))
    );
  }
}
