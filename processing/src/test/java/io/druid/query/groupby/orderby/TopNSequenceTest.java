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

package io.druid.query.groupby.orderby;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;


@RunWith(Parameterized.class)
public class TopNSequenceTest
{
  private static final long SEED = 2L;
  private static final Ordering<String> ASC = Ordering.natural();
  private static final Ordering<String> DESC = Ordering.natural().reverse();

  private static final List<String> EMPTY = Collections.EMPTY_LIST;
  private static final List<String> SINGLE = Lists.newArrayList("a");
  private static final List<String> RAW_ASC = Lists.newArrayList(Splitter.fixedLength(1).split("abcdefghijk"));
  private static final List<String> RAW_DESC = Lists.newArrayList(Splitter.fixedLength(1).split("kjihgfedcba"));

  private Ordering<String> ordering;
  private List<String> rawInput;
  private int limit;

  @Parameterized.Parameters
  public static Collection<Object[]> makeTestData()
  {
    Object[][] data = new Object[][] {
      { ASC, RAW_ASC, RAW_ASC.size() - 2},
      { ASC, RAW_ASC, RAW_ASC.size()},
      { ASC, RAW_ASC, RAW_ASC.size() + 2},
      { ASC, RAW_ASC, 0},
      { ASC, SINGLE, 0},
      { ASC, SINGLE, 1},
      { ASC, SINGLE, 2},
      { ASC, SINGLE, 3},
      { ASC, EMPTY, 0},
      { ASC, EMPTY, 1},
      { DESC, RAW_DESC, RAW_DESC.size() - 2},
      { DESC, RAW_DESC, RAW_DESC.size()},
      { DESC, RAW_DESC, RAW_DESC.size() + 2},
      { DESC, RAW_DESC, 0},
      { DESC, RAW_DESC, 0},
      { DESC, SINGLE, 1},
      { DESC, SINGLE, 2},
      { DESC, SINGLE, 3},
      { DESC, EMPTY, 0},
      { DESC, EMPTY, 1},
    };

    return Arrays.asList(data);
  }

  public TopNSequenceTest(Ordering<String> ordering, List<String> rawInput, int limit)
  {
    this.ordering = ordering;
    this.rawInput = rawInput;
    this.limit = limit;
  }

  @Test
  public void testOrderByWithLimit()
  {
    List<String> expected = rawInput.subList(0, Math.min(limit, rawInput.size()));
    List<String> inputs = Lists.newArrayList(rawInput);
    Collections.shuffle(inputs, new Random(2));

    Sequence<String> result = new TopNSequence<String>(Sequences.simple(inputs), ordering, limit);

    Assert.assertEquals(expected, Sequences.toList(result, Lists.<String>newArrayList()));
  }
}
