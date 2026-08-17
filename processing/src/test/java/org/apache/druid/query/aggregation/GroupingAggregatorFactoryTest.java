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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.constant.LongConstantAggregator;
import org.apache.druid.query.aggregation.constant.LongConstantBufferAggregator;
import org.apache.druid.query.aggregation.constant.LongConstantVectorAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;

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

  public static class NewAggregatorTests
  {
    private ColumnSelectorFactory metricFactory;

    @BeforeEach
    public void setup()
    {
      metricFactory = EasyMock.mock(ColumnSelectorFactory.class);
    }

    @Test
    public void testNewAggregator()
    {
      GroupingAggregatorFactory factory = makeFactory(new String[]{"a", "b"}, new String[]{"a"});
      Aggregator aggregator = factory.factorize(metricFactory);
      Assertions.assertEquals(LongConstantAggregator.class, aggregator.getClass());
      Assertions.assertEquals(1, aggregator.getLong());
    }

    @Test
    public void testNewBufferAggregator()
    {
      GroupingAggregatorFactory factory = makeFactory(new String[]{"a", "b"}, new String[]{"a"});
      BufferAggregator aggregator = factory.factorizeBuffered(metricFactory);
      Assertions.assertEquals(LongConstantBufferAggregator.class, aggregator.getClass());
      Assertions.assertEquals(1, aggregator.getLong(null, 0));
    }

    @Test
    public void testNewVectorAggregator()
    {
      GroupingAggregatorFactory factory = makeFactory(new String[]{"a", "b"}, new String[]{"a"});
      Assertions.assertTrue(factory.canVectorize(metricFactory));
      VectorAggregator aggregator = factory.factorizeVector(null);
      Assertions.assertEquals(LongConstantVectorAggregator.class, aggregator.getClass());
      Assertions.assertEquals(1L, aggregator.get(null, 0));
    }

    @Test
    public void testWithKeyDimensions()
    {
      GroupingAggregatorFactory factory = makeFactory(new String[]{"a", "b"}, new String[]{"a"});
      Aggregator aggregator = factory.factorize(metricFactory);
      Assertions.assertEquals(1, aggregator.getLong());
      factory = factory.withKeyDimensions(Sets.newHashSet("b"));
      aggregator = factory.factorize(metricFactory);
      Assertions.assertEquals(2, aggregator.getLong());
    }
  }

  public static class GroupingDimensionsTest
  {
    @Test
    public void testFactory_nullGroupingDimensions()
    {
      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> new GroupingAggregatorFactory("name", null, Sets.newHashSet("b"))
      );
      Assertions.assertTrue(exception.getMessage().contains("Must have a non-empty grouping dimensions"));
    }

    @Test
    public void testFactory_emptyGroupingDimensions()
    {
      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> makeFactory(new String[0], null)
      );
      Assertions.assertTrue(exception.getMessage().contains("Must have a non-empty grouping dimensions"));
    }

    @Test
    public void testFactory_highNumberOfGroupingDimensions()
    {
      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> makeFactory(new String[Long.SIZE], null)
      );
      Assertions.assertTrue(
          exception.getMessage().contains(StringUtils.format(
              "Number of dimensions %d is more than supported %d",
              Long.SIZE,
              Long.SIZE - 1
          ))
      );
    }

    @Test
    public void testWithDuplicateGroupings()
    {
      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> makeFactory(new String[]{"a", "a"}, null)
      );
      Assertions.assertTrue(
          exception.getMessage().contains("Encountered same dimension more than once in groupings")
      );
    }
  }

  @ParameterizedClass

  @MethodSource("arguments")
  public static class ValueTests
  {
    private final GroupingAggregatorFactory factory;
    private final long value;

    public ValueTests(String[] groupings, @Nullable String[] keyDimensions, long value)
    {
      factory = makeFactory(groupings, keyDimensions);
      this.value = value;
    }

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
      Assertions.assertEquals(value, factory.factorize(null).getLong());
    }
  }
}
