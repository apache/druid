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

package org.apache.druid.query.aggregation;

import com.google.common.collect.Sets;
import junitparams.converters.Nullable;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class GroupingAggregatorFactoryTest
{
  public static GroupingAggregatorFactory makeFactory(String[] groupings, @Nullable String[] keyDims)
  {
    GroupingAggregatorFactory factory = new GroupingAggregatorFactory("name", Arrays.asList(groupings));
    if (null != keyDims) {
      factory = factory.withKeyDimensions(Sets.newHashSet(keyDims));
    }
    return factory;
  }

  public static class GroupingDimensionsTest
  {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testFactory_nullGroupingDimensions()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("Must have a non-empty grouping dimensions");
      GroupingAggregatorFactory factory = new GroupingAggregatorFactory("name", null, Sets.newHashSet("b"));
    }

    @Test
    public void testFactory_emptyGroupingDimensions()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("Must have a non-empty grouping dimensions");
      makeFactory(new String[0], null);
    }

    @Test
    public void testFactory_highNumberOfGroupingDimensions()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage(StringUtils.format(
          "Number of dimensions %d is more than supported %d",
          Long.SIZE,
          Long.SIZE - 1
      ));
      makeFactory(new String[Long.SIZE], null);
    }
  }

  @RunWith(Parameterized.class)
  public static class ValueTests
  {
    private final GroupingAggregatorFactory factory;
    private final long value;

    public ValueTests(String[] groupings, @Nullable String[] keyDimensions, long value)
    {
      factory = makeFactory(groupings, keyDimensions);
      this.value = value;
    }

    @Parameterized.Parameters
    public static Collection arguments()
    {
      String[] maxGroupingList = new String[Long.SIZE - 1];
      for (int i = 0; i < maxGroupingList.length; i++) {
        maxGroupingList[i] = String.valueOf(i);
      }
      return Arrays.asList(new Object[][]{
          {new String[]{"a", "b"}, new String[0], 3},
          {new String[]{"a", "b"}, null, 0},
          {new String[]{"a", "b"}, new String[]{"a"}, 1},
          {new String[]{"a", "b"}, new String[]{"b"}, 2},
          {new String[]{"a", "b"}, new String[]{"a", "b"}, 0},
          {new String[]{"b", "a"}, new String[]{"a"}, 2},
          {maxGroupingList, null, 0},
          {maxGroupingList, new String[0], Long.MAX_VALUE}
      });
    }

    @Test
    public void testValue()
    {
      Assert.assertEquals(value, factory.getValue());
    }
  }
}
