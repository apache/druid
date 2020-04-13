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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.groupby.epinephelinae.BufferHashGrouper;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BufferHashGrouperUsingSketchMergeAggregatorFactoryTest
{
  private static BufferHashGrouper<Integer> makeGrouper(
      TestColumnSelectorFactory columnSelectorFactory,
      int bufferSize,
      int initialBuckets
  )
  {
    final BufferHashGrouper<Integer> grouper = new BufferHashGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(bufferSize)),
        GrouperTestUtil.intKeySerde(),
        AggregatorAdapters.factorizeBuffered(
            columnSelectorFactory,
            ImmutableList.of(
                new SketchMergeAggregatorFactory("sketch", "sketch", 16, false, true, 2),
                new CountAggregatorFactory("count")
            )
        ),
        Integer.MAX_VALUE,
        0.75f,
        initialBuckets,
        true
    );
    grouper.init();
    return grouper;
  }

  @Test
  public void testGrowingBufferGrouper()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    final Grouper<Integer> grouper = makeGrouper(columnSelectorFactory, 100000, 2);
    try {
      final int expectedMaxSize = 5;

      SketchHolder sketchHolder = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(16).build());
      UpdateSketch updateSketch = (UpdateSketch) sketchHolder.getSketch();
      updateSketch.update(1);

      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));

      for (int i = 0; i < expectedMaxSize; i++) {
        Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
      }

      updateSketch.update(3);
      columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));

      for (int i = 0; i < expectedMaxSize; i++) {
        Assert.assertTrue(String.valueOf(i), grouper.aggregate(i).isOk());
      }

      Object[] holders = Lists.newArrayList(grouper.iterator(true)).get(0).getValues();

      Assert.assertEquals(2.0d, ((SketchHolder) holders[0]).getEstimate(), 0);
    }
    finally {
      grouper.close();
    }
  }

}
