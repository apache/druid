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

package io.druid.java.util.common.guava;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.StringUtils;
import org.junit.Test;

import java.util.List;

/**
 */
public class FilteredSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    Predicate<Integer> pred = new Predicate<Integer>()
    {
      @Override
      public boolean apply(Integer input)
      {
        return input % 3 == 0;
      }
    };

    for (int i = 0; i < 25; ++i) {
      List<Integer> vals = Lists.newArrayList();
      for (int j = 0; j < i; ++j) {
        vals.add(j);
      }

      SequenceTestHelper.testAll(
          StringUtils.format("Run %,d: ", i),
          new FilteredSequence<>(Sequences.simple(vals), pred),
          Lists.newArrayList(Iterables.filter(vals, pred))
      );
    }
  }
}
