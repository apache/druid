/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.guava.nary;

import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.Comparators;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class SortedMergeIteratorTest
{
  @Test
  public void testSanity() throws Exception
  {
    SortedMergeIterator<Integer, Integer> iter = SortedMergeIterator.create(
        Arrays.asList(1, 4, 5, 7, 9).iterator(),
        Arrays.asList(1, 2, 3, 6, 7, 8, 9, 10, 11).iterator(),
        Comparators.<Integer>comparable(),
        new BinaryFn<Integer, Integer, Integer>()
        {
          @Override
          public Integer apply(Integer arg1, Integer arg2)
          {
            return arg1 == null ? arg2 : arg2 == null ? arg1 : arg1 + arg2;
          }
        }
    );

    Assert.assertEquals(
        Arrays.asList(2, 2, 3, 4, 5, 6, 14, 8, 18, 10, 11),
        Lists.newArrayList(iter)
    );
  }
}
