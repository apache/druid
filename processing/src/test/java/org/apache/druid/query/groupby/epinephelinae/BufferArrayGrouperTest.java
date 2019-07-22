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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class BufferArrayGrouperTest
{
  @Test
  public void testAggregate()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final IntGrouper grouper = newGrouper(columnSelectorFactory, 32768);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 10L)));
    grouper.aggregate(12);
    grouper.aggregate(6);
    grouper.aggregate(10);
    grouper.aggregate(6);
    grouper.aggregate(12);
    grouper.aggregate(6);

    final List<Entry<Integer>> expected = ImmutableList.of(
        new Grouper.Entry<>(6, new Object[]{30L, 3L}),
        new Grouper.Entry<>(10, new Object[]{10L, 1L}),
        new Grouper.Entry<>(12, new Object[]{20L, 2L})
    );
    final List<Entry<Integer>> unsortedEntries = Lists.newArrayList(grouper.iterator(false));

    Assert.assertEquals(
        expected,
        Ordering.from((Comparator<Entry<Integer>>) (o1, o2) -> Ints.compare(o1.getKey(), o2.getKey()))
                .sortedCopy(unsortedEntries)
    );
  }

  private BufferArrayGrouper newGrouper(
      TestColumnSelectorFactory columnSelectorFactory,
      int bufferSize
  )
  {
    final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    final BufferArrayGrouper grouper = new BufferArrayGrouper(
        Suppliers.ofInstance(buffer),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new LongSumAggregatorFactory("valueSum", "value"),
                new CountAggregatorFactory("count")
            )
        ),
        1000
    );
    grouper.init();
    return grouper;
  }

  @Test
  public void testRequiredBufferCapacity()
  {
    int[] cardinalityArray = new int[]{1, 10, Integer.MAX_VALUE - 1};
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new LongSumAggregatorFactory("sum", "sum")
    };
    long[] requiredSizes;
    if (NullHandling.sqlCompatible()) {
      // We need additional size to store nullability information.
      requiredSizes = new long[]{19, 101, 19058917368L};
    } else {
      requiredSizes = new long[]{17, 90, 16911433721L};
    }

    for (int i = 0; i < cardinalityArray.length; i++) {
      Assert.assertEquals(requiredSizes[i], BufferArrayGrouper.requiredBufferCapacity(
          cardinalityArray[i],
          aggregatorFactories
      ));
    }
  }
}
